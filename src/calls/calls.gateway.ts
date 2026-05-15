import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
  WsException,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { JwtService } from '@nestjs/jwt';
import { ConfigService } from '@nestjs/config';
import { CallsService } from './calls.service';
import { CallInviteDto } from './dto/call-invite.dto';
import { CallIdDto } from './dto/call-id.dto';
import { CallEndDto } from './dto/call-end.dto';
import { WebRtcSignalDto } from './dto/webrtc-signal.dto';
import { getAccessTokenFromRequest } from '../common/utils/auth-token.util';

@WebSocketGateway({
  namespace: '/calls',
  cors: {
    origin: true,
    credentials: true,
  },
})
export class CallsGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(CallsGateway.name);
  private readonly unansweredTimeouts = new Map<string, NodeJS.Timeout>();

  constructor(
    private readonly jwtService: JwtService,
    private readonly configService: ConfigService,
    private readonly callsService: CallsService,
  ) {}

  async handleConnection(client: Socket) {
    try {
      const tokenFromAuth = client.handshake.auth?.token;
      const tokenFromRequest = getAccessTokenFromRequest(client.handshake as any);
      const token = tokenFromAuth || tokenFromRequest;

      if (!token) {
        throw new WsException('Authentication token is required');
      }

      const payload = this.jwtService.verify(token, {
        secret: this.configService.get<string>('JWT_SECRET'),
      });

      if (!payload?.sub) {
        throw new WsException('Invalid token payload');
      }

      const userId = String(payload.sub);
      client.data.userId = userId;
      client.join(`user:${userId}`);
      this.logger.debug(`Calls socket connected for user ${userId}: ${client.id}`);
    } catch (error) {
      this.logger.warn(`Calls socket auth failed: ${error.message}`);
      client.emit('call_error', { message: 'Authentication failed' });
      client.disconnect();
    }
  }

  handleDisconnect(client: Socket) {
    this.logger.debug(`Calls socket disconnected: ${client.id}`);
  }

  private otherParticipant(call: any, userId: string) {
    return call.participants.find((participantId: string) => String(participantId) !== String(userId));
  }

  private scheduleUnansweredTimeout(callId: string) {
    this.clearUnansweredTimeout(callId);

    const timeout = setTimeout(async () => {
      try {
        const call = await this.callsService.markMissedIfUnanswered(callId);
        if (!call || call.status !== 'missed') return;

        const event = {
          callId: call.callId,
          by: call.callerId,
          status: call.status,
          endedAt: call.endedAt,
          durationSec: call.durationSec || 0,
          reason: call.endReason || 'timeout',
        };

        for (const participantId of call.participants || []) {
          this.server.to(`user:${participantId}`).emit('call_ended', event);
        }
      } catch (error) {
        this.logger.warn(`Unanswered timeout emit failed for ${callId}: ${error.message}`);
      } finally {
        this.unansweredTimeouts.delete(callId);
      }
    }, 30000);

    this.unansweredTimeouts.set(callId, timeout);
  }

  private clearUnansweredTimeout(callId: string) {
    const timeout = this.unansweredTimeouts.get(callId);
    if (!timeout) return;

    clearTimeout(timeout);
    this.unansweredTimeouts.delete(callId);
  }

  @SubscribeMessage('call_invite')
  async inviteCall(@ConnectedSocket() client: Socket, @MessageBody() payload: CallInviteDto) {
    try {
      const callerId = client.data.userId as string;
      if (!callerId) throw new WsException('Unauthorized');

      const result = await this.callsService.createCallInvite(
        callerId,
        payload.conversationId,
        payload.calleeId,
        payload.type,
      );

      if (result.busy) {
        this.server.to(`user:${callerId}`).emit('call_busy', {
          calleeId: payload.calleeId,
          conversationId: payload.conversationId,
        });

        return {
          success: true,
          data: { busy: true },
        };
      }

      const call = result.call;
      this.scheduleUnansweredTimeout(call.callId);

      this.server.to(`user:${payload.calleeId}`).emit('incoming_call', {
        callId: call.callId,
        conversationId: call.conversationId,
        callerId: call.callerId,
        calleeId: call.calleeId,
        type: call.type,
        status: call.status,
        startedAt: call.startedAt,
      });

      this.server.to(`user:${callerId}`).emit('call_ringing', {
        callId: call.callId,
        calleeId: call.calleeId,
        conversationId: call.conversationId,
      });

      return {
        success: true,
        data: {
          callId: call.callId,
          status: call.status,
        },
      };
    } catch (error) {
      throw new WsException(error.message || 'Failed to initiate call');
    }
  }

  @SubscribeMessage('call_accept')
  async acceptCall(@ConnectedSocket() client: Socket, @MessageBody() payload: CallIdDto) {
    try {
      const userId = client.data.userId as string;
      if (!userId) throw new WsException('Unauthorized');

      const call = await this.callsService.acceptCall(payload.callId, userId);
      this.clearUnansweredTimeout(call.callId);
      const otherUserId = this.otherParticipant(call, userId);

      this.server.to(`user:${userId}`).emit('call_accepted', {
        callId: call.callId,
        by: userId,
        answeredAt: call.answeredAt,
      });

      if (otherUserId) {
        this.server.to(`user:${otherUserId}`).emit('call_accepted', {
          callId: call.callId,
          by: userId,
          answeredAt: call.answeredAt,
        });
      }

      return { success: true, data: { callId: call.callId, status: call.status } };
    } catch (error) {
      throw new WsException(error.message || 'Failed to accept call');
    }
  }

  @SubscribeMessage('call_reject')
  async rejectCall(@ConnectedSocket() client: Socket, @MessageBody() payload: CallIdDto) {
    try {
      const userId = client.data.userId as string;
      if (!userId) throw new WsException('Unauthorized');

      const call = await this.callsService.rejectCall(payload.callId, userId);
      this.clearUnansweredTimeout(call.callId);
      const otherUserId = this.otherParticipant(call, userId);

      const event = {
        callId: call.callId,
        by: userId,
        status: call.status,
        reason: call.endReason,
      };

      this.server.to(`user:${userId}`).emit('call_rejected', event);
      if (otherUserId) this.server.to(`user:${otherUserId}`).emit('call_rejected', event);

      return { success: true, data: event };
    } catch (error) {
      throw new WsException(error.message || 'Failed to reject call');
    }
  }

  @SubscribeMessage('call_end')
  async endCall(@ConnectedSocket() client: Socket, @MessageBody() payload: CallEndDto) {
    try {
      const userId = client.data.userId as string;
      if (!userId) throw new WsException('Unauthorized');

      const call = await this.callsService.endCall(payload.callId, userId, payload.reason || 'hangup');
      this.clearUnansweredTimeout(call.callId);
      const otherUserId = this.otherParticipant(call, userId);

      const event = {
        callId: call.callId,
        by: userId,
        status: call.status,
        endedAt: call.endedAt,
        durationSec: call.durationSec,
        reason: call.endReason,
      };

      this.server.to(`user:${userId}`).emit('call_ended', event);
      if (otherUserId) this.server.to(`user:${otherUserId}`).emit('call_ended', event);

      return { success: true, data: event };
    } catch (error) {
      throw new WsException(error.message || 'Failed to end call');
    }
  }

  @SubscribeMessage('webrtc_offer')
  async relayOffer(@ConnectedSocket() client: Socket, @MessageBody() payload: WebRtcSignalDto) {
    return this.relaySignal(client, payload, 'webrtc_offer');
  }

  @SubscribeMessage('webrtc_answer')
  async relayAnswer(@ConnectedSocket() client: Socket, @MessageBody() payload: WebRtcSignalDto) {
    return this.relaySignal(client, payload, 'webrtc_answer');
  }

  @SubscribeMessage('webrtc_ice_candidate')
  async relayIce(@ConnectedSocket() client: Socket, @MessageBody() payload: WebRtcSignalDto) {
    return this.relaySignal(client, payload, 'webrtc_ice_candidate');
  }

  private async relaySignal(client: Socket, payload: WebRtcSignalDto, eventName: string) {
    try {
      const userId = client.data.userId as string;
      if (!userId) throw new WsException('Unauthorized');

      const call = await this.callsService.ensureParticipant(payload.callId, userId);
      const targetUserId = payload.targetUserId || this.otherParticipant(call, userId);

      if (!targetUserId) {
        throw new WsException('Target user not found');
      }

      this.server.to(`user:${targetUserId}`).emit(eventName, {
        callId: call.callId,
        fromUserId: userId,
        signal: payload.signal,
      });

      return { success: true };
    } catch (error) {
      throw new WsException(error.message || `Failed to relay ${eventName}`);
    }
  }
}
