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
import { Logger, NotFoundException } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { JwtService } from '@nestjs/jwt';
import { ConfigService } from '@nestjs/config';
import { ChatsService } from './chats.service';
import { getAccessTokenFromRequest } from '../common/utils/auth-token.util';

@WebSocketGateway({
	namespace: '/chat',
	cors: {
		origin: true,
		credentials: true,
	},
})
export class ChatsGateway implements OnGatewayConnection, OnGatewayDisconnect {
	@WebSocketServer()
	server: Server;

	private readonly logger = new Logger(ChatsGateway.name);
	private readonly onlineUsers = new Map<string, Set<string>>();
	private readonly lastSeenMap = new Map<string, string>();

	constructor(
		private jwtService: JwtService,
		private configService: ConfigService,
		private chatsService: ChatsService,
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
			this.trackOnline(userId, client.id);
			this.server.emit('presence_state', {
				userId,
				online: true,
				lastSeenAt: null,
			});

			this.logger.debug(`Socket connected for user ${userId}: ${client.id}`);
		} catch (error) {
			this.logger.warn(`Socket auth failed: ${error.message}`);
			client.emit('chat_error', { message: 'Authentication failed' });
			client.disconnect();
		}
	}

	handleDisconnect(client: Socket) {
		const userId = client.data?.userId as string | undefined;
		if (userId) {
			const isStillOnline = this.trackOffline(userId, client.id);
			if (!isStillOnline) {
				const lastSeenAt = new Date().toISOString();
				this.lastSeenMap.set(userId, lastSeenAt);
				this.server.emit('presence_state', {
					userId,
					online: false,
					lastSeenAt,
				});
			}
		}
		this.logger.debug(`Socket disconnected: ${client.id}`);
	}

	private trackOnline(userId: string, socketId: string) {
		if (!this.onlineUsers.has(userId)) {
			this.onlineUsers.set(userId, new Set());
		}
		this.onlineUsers.get(userId)?.add(socketId);
	}

	private trackOffline(userId: string, socketId: string) {
		const sockets = this.onlineUsers.get(userId);
		if (!sockets) return false;
		sockets.delete(socketId);
		if (sockets.size === 0) {
			this.onlineUsers.delete(userId);
			return false;
		}
		return true;
	}

	private isUserOnline(userId: string) {
		return this.onlineUsers.has(String(userId));
	}

	@SubscribeMessage('join_conversation')
	async joinConversation(
		@ConnectedSocket() client: Socket,
		@MessageBody() payload: { conversationId: string },
	) {
		try {
			const userId = client.data.userId as string;
			if (!userId) {
				throw new WsException('Unauthorized');
			}

			const conversation = await this.chatsService.getConversationForUser(userId, payload.conversationId);
			const room = `conversation:${conversation._id.toString()}`;
			client.join(room);

			const otherUserId = conversation.participants.find((id) => id !== String(userId)) || null;
			if (otherUserId) {
				const lastSeenAt = this.lastSeenMap.get(otherUserId) || null;
				client.emit('presence_state', {
					userId: otherUserId,
					online: this.isUserOnline(otherUserId),
					lastSeenAt,
				});
			}

			return {
				success: true,
				room,
			};
		} catch (error) {
			throw new WsException(error.message || 'Failed to join conversation');
		}
	}

	@SubscribeMessage('leave_conversation')
	async leaveConversation(
		@ConnectedSocket() client: Socket,
		@MessageBody() payload: { conversationId: string },
	) {
		const room = `conversation:${payload.conversationId}`;
		client.leave(room);
		return { success: true };
	}

	@SubscribeMessage('send_message')
	async sendMessage(
		@ConnectedSocket() client: Socket,
		@MessageBody()
		payload: {
			conversationId: string;
			text?: string;
			adId?: string;
			attachments?: Array<{
				name: string;
				mimeType: string;
				url: string;
				type?: 'image' | 'file';
				size?: number;
			}>;
		},
	) {
		try {
			const userId = client.data.userId as string;
			if (!userId) {
				throw new WsException('Unauthorized');
			}

			const message = await this.chatsService.sendMessage(userId, payload.conversationId, {
				text: payload.text,
				adId: payload.adId,
				attachments: payload.attachments,
			});

			const room = `conversation:${payload.conversationId}`;
			this.server.to(room).emit('new_message', message);

			const participants = Array.isArray((message as any).participants)
				? (message as any).participants
				: [];

			for (const participantId of participants) {
				this.server.to(`user:${participantId}`).emit('conversation_updated', {
					conversationId: payload.conversationId,
					lastMessageText: message.text || (Array.isArray(message.attachments) && message.attachments.length ? '📎 Attachment' : ''),
					lastMessageAt: message.createdAt,
					lastMessageAdId: message.adId,
					lastMessageAdTitle: message.adTitle || null,
					message,
				});
				if (String(participantId) !== String(userId)) {
					this.server.to(`user:${participantId}`).emit('presence_state', {
						userId,
						online: this.isUserOnline(userId),
						lastSeenAt: this.lastSeenMap.get(userId) || null,
					});
				}
			}

			return {
				success: true,
				data: message,
			};
		} catch (error) {
			throw new WsException(error.message || 'Failed to send message');
		}
	}

	@SubscribeMessage('typing_start')
	async typingStart(
		@ConnectedSocket() client: Socket,
		@MessageBody() payload: { conversationId: string },
	) {
		const userId = client.data.userId as string;
		if (!userId || !payload?.conversationId) return { success: false };

		const context = await this.chatsService.getConversationPresenceForUser(userId, payload.conversationId);
		const room = `conversation:${payload.conversationId}`;
		client.to(room).emit('typing_state', {
			conversationId: payload.conversationId,
			userId,
			isTyping: true,
		});

		if (context.otherUserId) {
			this.server.to(`user:${context.otherUserId}`).emit('typing_state', {
				conversationId: payload.conversationId,
				userId,
				isTyping: true,
			});
		}

		return { success: true };
	}

	@SubscribeMessage('typing_stop')
	async typingStop(
		@ConnectedSocket() client: Socket,
		@MessageBody() payload: { conversationId: string },
	) {
		const userId = client.data.userId as string;
		if (!userId || !payload?.conversationId) return { success: false };

		const context = await this.chatsService.getConversationPresenceForUser(userId, payload.conversationId);
		const room = `conversation:${payload.conversationId}`;
		client.to(room).emit('typing_state', {
			conversationId: payload.conversationId,
			userId,
			isTyping: false,
		});

		if (context.otherUserId) {
			this.server.to(`user:${context.otherUserId}`).emit('typing_state', {
				conversationId: payload.conversationId,
				userId,
				isTyping: false,
			});
		}

		return { success: true };
	}

	@SubscribeMessage('mark_read')
	async markRead(
		@ConnectedSocket() client: Socket,
		@MessageBody() payload: { conversationId: string },
	) {
		const userId = client.data.userId as string;
		if (!userId || !payload?.conversationId) return { success: false };

		let result: { messageIds: string[] };
		try {
			result = await this.chatsService.markConversationAsRead(userId, payload.conversationId);
		} catch (error) {
			if (error instanceof NotFoundException) {
				this.logger.warn(
					`mark_read ignored: conversation ${payload.conversationId} not found for user ${userId}`,
				);
				return {
					success: true,
					data: { messageIds: [] },
				};
			}
			throw error;
		}
		if (result.messageIds.length > 0) {
			const room = `conversation:${payload.conversationId}`;
			this.server.to(room).emit('messages_read', {
				conversationId: payload.conversationId,
				readerId: userId,
				messageIds: result.messageIds,
				readAt: new Date().toISOString(),
			});
		}

		return {
			success: true,
			data: {
				messageIds: result.messageIds,
			},
		};
	}
}
