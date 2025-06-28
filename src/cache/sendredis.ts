import { createHash } from 'crypto';
import { redisClient } from './rediscache.client';

// Cache global para deduplicação de mensagens
const messageCache = new Set<string>();
const cacheTimestamps = new Map<string, number>();
const MAX_CACHE_SIZE = 10000;
const CACHE_TTL = 300000; // 5 minutos

interface ChatwootInfo {
  enabled?: boolean;
  accountId?: string;
  url?: string;
  conversationId?: string;
  messageId?: string;
  inboxId?: string;
}

/**
 * Função para enviar eventos ao Redis Stream com deduplicação e proteção contra nulos
 * Segue o padrão da Evolution API - NUNCA lança erros, apenas retorna status
 */
async function sendRedisEvent(
  eventType: string,
  data: any,
  instanceName?: string,
  streamName?: string,
  chatwootInfo?: ChatwootInfo
): Promise<{
  success: boolean;
  reason: string;
  eventId?: string;
  sent: boolean;
}> {
  try {
    // Pega o nome da stream do .env ou usa valor padrão
    const defaultStreamName = process.env.REDIS_STREAM || 'evolution:events';
    const finalStreamName = streamName || defaultStreamName;

    this.logger.info(`Sending event to Redis Stream: ${finalStreamName}, Type: ${eventType}`);
    
    // 1. VALIDAÇÃO RIGOROSA DE NULOS/UNDEFINED
    const validation = validateInput(eventType, data);
    if (!validation.isValid) {
      return {
        success: false,
        reason: 'invalid_input',
        sent: false
      };
    }

    // 2. LIMPEZA PERIÓDICA DO CACHE (prevenir memory leak)
    cleanExpiredCache();

    // 3. SERIALIZAÇÃO SEGURA E CRIAÇÃO DE HASH
    const serializedData = safeStringify(data);
    const messageHash = createMessageHash(eventType, serializedData, instanceName);

    // 4. VERIFICAÇÃO DE DUPLICATA
    if (isDuplicate(messageHash)) {
      return {
        success: false,
        reason: 'duplicate_message',
        sent: false
      };
    }

    // 5. PREPARAÇÃO DO PAYLOAD COM INFORMAÇÕES DO CHATWOOT
    const eventPayload: any = {
      type: eventType,
      data: serializedData,
      timestamp: Date.now().toString(),
      instance: instanceName || 'global'
    };

    // Adiciona informações do Chatwoot se disponíveis
    if (chatwootInfo?.enabled && chatwootInfo?.accountId) {
      eventPayload.chatwoot = {
        accountId: chatwootInfo.accountId,
        url: chatwootInfo.url,
        conversationId: chatwootInfo.conversationId,
        messageId: chatwootInfo.messageId,
        inboxId: chatwootInfo.inboxId
      };
    }

    // 6. ENVIO PARA REDIS STREAM
    const client = redisClient.getConnection();
    if (!client) {
      return {
        success: false,
        reason: 'redis_not_available',
        sent: false
      };
    }

    const eventId = await client.xAdd(
      finalStreamName,
      '*', // ID automático
      eventPayload
    );
    console.log(`Event sent to Redis Stream: ${finalStreamName}, ID: ${eventId}`);

    // 7. REGISTRO NO CACHE APÓS SUCESSO
    addToCache(messageHash);

    return {
      success: true,
      reason: 'sent_successfully',
      eventId: eventId,
      sent: true
    };

  } catch (error) {
    // NUNCA LANÇA ERRO - apenas retorna falha
    return {
      success: false,
      reason: 'redis_stream_error',
      sent: false
    };
  }
}

/**
 * Validação específica para conteúdo de mensagens - NUNCA deve lançar erro
 * Garante que apenas mensagens com texto real sejam enviadas
 */
function validateMessageContent(data: any): { isValid: boolean; reason?: string } {
  try {
    // Se não há dados, rejeita
    if (!data) {
      return { isValid: false, reason: 'no_message_data' };
    }

    let message = null;

    // Extrai o objeto message de diferentes estruturas possíveis
    if (data.message) {
      message = data.message;
    } else if (data.messages && Array.isArray(data.messages) && data.messages[0]?.message) {
      message = data.messages[0].message;
    } else {
      return { isValid: false, reason: 'no_message_object' };
    }

    // Verifica se tem messageStubType (eventos de sistema - REJEITAR)
    if (data.messageStubType !== undefined || (data.messages?.[0]?.messageStubType !== undefined)) {
      return { isValid: false, reason: 'system_message_stub' };
    }

    // Lista de propriedades que indicam mensagem com texto válido
    const validTextProperties = [
      'conversation',                    // Texto simples
      'extendedTextMessage',            // Texto com formatação/link
      'imageMessage',                   // Imagem (pode ter caption)
      'videoMessage',                   // Vídeo (pode ter caption)
      'documentMessage',                // Documento (pode ter caption)
      'audioMessage'                    // Áudio (mensagem válida)
    ];

    // Verifica se existe pelo menos uma propriedade de texto válido
    const hasValidContent = validTextProperties.some(prop => {
      if (message[prop]) {
        // Para conversation, verifica se tem texto não vazio
        if (prop === 'conversation') {
          return message[prop] && typeof message[prop] === 'string' && message[prop].trim() !== '';
        }
        
        // Para extendedTextMessage, verifica se tem texto
        if (prop === 'extendedTextMessage') {
          return message[prop].text && typeof message[prop].text === 'string' && message[prop].text.trim() !== '';
        }
        
        // Para mídias, considera válido (mesmo sem caption)
        if (['imageMessage', 'videoMessage', 'documentMessage', 'audioMessage'].includes(prop)) {
          return true;
        }
        
        return true;
      }
      return false;
    });

    if (!hasValidContent) {
      return { isValid: false, reason: 'no_valid_text_content' };
    }

    // Rejeita tipos específicos de mensagem que não queremos
    const rejectMessageTypes = [
      'protocolMessage',      // Mensagens de protocolo
      'pollUpdateMessage',    // Atualizações de enquete
      'reactionMessage',      // Apenas reações (sem texto novo)
      'senderKeyDistributionMessage',  // Distribuição de chaves
      'messageContextInfo'    // Apenas contexto
    ];

    const hasRejectedType = rejectMessageTypes.some(type => message[type]);
    if (hasRejectedType) {
      return { isValid: false, reason: 'rejected_message_type' };
    }

    return { isValid: true };

  } catch {
    return { isValid: false, reason: 'validation_error' };
  }
}

/**
 * Validação rigorosa de entrada - NUNCA deve lançar erro
 */
function validateInput(eventType: any, data: any): { isValid: boolean; errors?: string[] } {
  try {
    const errors: string[] = [];

    // Verificação de eventType
    if (!eventType || 
        typeof eventType !== 'string' || 
        eventType.trim() === '' ||
        eventType === 'null' ||
        eventType === 'undefined') {
      errors.push('eventType_invalid');
    }

    // Verificação de data - permite valores falsy válidos (0, false, '')
    if (data === null || 
        data === undefined ||
        (typeof data === 'string' && (data === 'null' || data === 'undefined'))) {
      errors.push('data_null_or_undefined');
    }

    return {
      isValid: errors.length === 0,
      errors: errors.length > 0 ? errors : undefined
    };
  } catch {
    return { isValid: false, errors: ['validation_error'] };
  }
}

/**
 * Serialização segura que nunca falha
 */
function safeStringify(data: any): string {
  try {
    if (data === null) return 'null';
    if (data === undefined) return 'undefined';
    if (typeof data === 'string') return data;
    if (typeof data === 'number' || typeof data === 'boolean') return String(data);
    
    return JSON.stringify(data, (key, value) => {
      // Substitui valores problemáticos
      if (value === null) return '__NULL__';
      if (value === undefined) return '__UNDEFINED__';
      if (typeof value === 'function') return '__FUNCTION__';
      if (value instanceof Date) return value.toISOString();
      if (typeof value === 'bigint') return value.toString();
      return value;
    });
  } catch {
    return `__STRINGIFY_ERROR__:${typeof data}`;
  }
}

/**
 * Criação de hash para detecção de duplicatas
 */
function createMessageHash(eventType: string, data: string, instanceName?: string): string {
  try {
    const content = `${eventType}:${data}:${instanceName || 'global'}`;
    return createHash('sha256').update(content).digest('hex').substring(0, 16);
  } catch {
    return `fallback_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  }
}

/**
 * Verificação de duplicata thread-safe
 */
function isDuplicate(hash: string): boolean {
  try {
    return messageCache.has(hash);
  } catch {
    return false; // Em caso de erro, assume que não é duplicata
  }
}

/**
 * Adiciona ao cache com controle de memória
 */
function addToCache(hash: string): void {
  try {
    // Controle de tamanho do cache
    if (messageCache.size >= MAX_CACHE_SIZE) {
      const oldestItems = Array.from(messageCache).slice(0, Math.floor(MAX_CACHE_SIZE * 0.3));
      oldestItems.forEach(item => {
        messageCache.delete(item);
        cacheTimestamps.delete(item);
      });
    }

    messageCache.add(hash);
    cacheTimestamps.set(hash, Date.now());
  } catch {
    // Falha silenciosa - não afeta o funcionamento principal
  }
}

/**
 * Limpeza de cache expirado
 */
function cleanExpiredCache(): void {
  try {
    const now = Date.now();
    const expiredItems: string[] = [];

    cacheTimestamps.forEach((timestamp, hash) => {
      if (now - timestamp > CACHE_TTL) {
        expiredItems.push(hash);
      }
    });

    expiredItems.forEach(hash => {
      messageCache.delete(hash);
      cacheTimestamps.delete(hash);
    });
  } catch {
    // Falha silenciosa
  }
}

export { sendRedisEvent };