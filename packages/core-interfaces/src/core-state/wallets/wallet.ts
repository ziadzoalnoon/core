import { Interfaces, Utils } from "@arkecosystem/crypto";

export interface IWallet {
    address: string;
    publicKey: string | undefined;
    secondPublicKey: string | undefined;
    balance: Utils.BigNumber;
    vote: string;
    voted: boolean;
    username: string | undefined;
    lastBlock: any;
    voteBalance: Utils.BigNumber;
    multisignature?: Interfaces.IMultiSignatureAsset;
    dirty: boolean;
    producedBlocks: number;
    forgedFees: Utils.BigNumber;
    forgedRewards: Utils.BigNumber;
    rate?: number;

    applyBlock(block: Interfaces.IBlockData): boolean;
    revertBlock(block: Interfaces.IBlockData): boolean;

    auditApply(transaction: Interfaces.ITransactionData): any[];
    toString(): string;

    verifySignatures(
        transaction: Interfaces.ITransactionData,
        multisignature?: Interfaces.IMultiSignatureAsset,
    ): boolean;
}

export type IDelegateWallet = IWallet & { rate: number; round: number };
