import { State, TransactionPool } from '@arkecosystem/core-interfaces';
import { Interfaces } from '@arkecosystem/crypto';

export interface ITransactionWorkerJob {
    ticketId: string,
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