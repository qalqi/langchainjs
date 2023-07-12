import * as firebaseAdmin from "firebase-admin";
import {
  getFirestore,
  DocumentData,
  Firestore,
  DocumentReference,
} from "firebase-admin/firestore";
import {
  StoredMessage,
  BaseMessage,
  BaseListChatMessageHistory,
} from "../../schema/index.js";
import {
  mapChatMessagesToStoredMessages,
  mapStoredMessagesToChatMessages,
} from "./utils.js";

export interface FirestoreDBChatMessageHistory {
  collectionName: string;
  sessionId: string;
  userId: string;
  config?: firebaseAdmin.AppOptions;
}
export class FirestoreChatMessageHistory extends BaseListChatMessageHistory {
  lc_namespace = ["langchain", "stores", "message", "firestore"];

  private collectionName: string;

  private sessionId: string;

  private userId: string;

  private config: firebaseAdmin.AppOptions;

  private firestoreClient: Firestore;

  private document: DocumentReference<DocumentData> | null;

  private messages: BaseMessage[];

  constructor({
    collectionName,
    sessionId,
    userId,
    config,
  }: FirestoreDBChatMessageHistory) {
    super();
    this.collectionName = collectionName;
    this.sessionId = sessionId;
    this.userId = userId;
    this.document = null;
    this.messages = [];
    if (config) this.config = config;

    this.prepareFirestore();
  }

  private prepareFirestore(): void {
    try {
      firebaseAdmin.app(); // Check if the app is already initialized
    } catch (e) {
      firebaseAdmin.initializeApp(this.config);
    }
    this.firestoreClient = getFirestore(firebaseAdmin.app());
    this.document = this.firestoreClient
      .collection(this.collectionName)
      .doc(this.sessionId);
    this.getMessages().catch((err) => {
      throw new Error(`Unknown response type: ${err.toString()}`);
    });
  }

  async getMessages(): Promise<BaseMessage[]> {
    if (!this.document) {
      throw new Error("Document not initialized");
    }

    const doc = await this.document.get();
    if (doc.exists) {
      const data = doc.data();
      if (data?.messages && data.messages.length > 0) {
        this.messages = mapStoredMessagesToChatMessages(data.messages);
      }
    }
    return this.messages;
  }

  protected async addMessage(message: BaseMessage) {
    const messages = mapChatMessagesToStoredMessages([message]);

    this.messages.push(message);
    await this.upsertMessages(messages);
  }

  private upsertMessages(messages: StoredMessage[]): void {
    if (!this.document) {
      throw new Error("Document not initialized");
    }

    this.document
      .set(
        {
          id: this.sessionId,
          user_id: this.userId,
          messages: [...messages],
        },
        { merge: true }
      )
      .catch((err) => {
        throw new Error(`Unknown response type: ${err.toString()}`);
      });
  }

  public clear(): void {
    this.messages = [];
    if (this.document) {
      this.document.delete().catch((err) => {
        throw new Error(`Unknown response type: ${err.toString()}`);
      });
    }
  }
}
