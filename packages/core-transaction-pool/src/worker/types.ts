import { State, TransactionPool } from '@arkecosystem/core-interfaces';
import { Interfaces } from '@arkecosystem/crypto';

export enum BrokerToWorker {
    Initialize = "initialize",
    CreateJob = "createJob",
    BlockHeightUpdate = "blockHeightUpdate",
}

export enum WorkerToBroker {
    TicketId = "ticketId",
    TransactionJobResult = "transactionJobResult",
    Unknown = "unknown",
}

export interface IMessageObject<T> {
    type: T;
    data: any;
}

export interface ITransactionWorkerJob {
    transactions: ReadonlyArray<Interfaces.ITransactionData>;
    senderWallets: Record<string, State.IWallet>;
}

export interface IPendingTransactionJobResult {
    ticketId: string;
    validTransactions: Array<{ buffer: Buffer, id: string }>;
    accept?: { [id: string]: Interfaces.ITransaction };
    broadcast?: { [id: string]: Interfaces.ITransaction };
    invalid: { [id: string]: boolean };
    excess: { [id: string]: boolean };
    errors: { [key: string]: TransactionPool.ITransactionErrorResponse[] };
}

export interface IQueuedTransactionJob extends ITransactionWorkerJob {
    ticketId: string;
}