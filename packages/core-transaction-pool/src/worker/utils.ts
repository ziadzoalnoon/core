import { IPendingTransactionJobResult } from './types';

export type PartialJobResult = Pick<IPendingTransactionJobResult, "errors" | "invalid">

export const pushError = (result: PartialJobResult, transactionId: string, error: { type: string, message: string }): void => {
    if (!result.errors[transactionId]) {
        result.errors[transactionId] = [];
    }

    result.errors[transactionId].push(error);
    result.invalid[transactionId] = true;
}