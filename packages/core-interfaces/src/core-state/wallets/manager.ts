import { Interfaces } from "@arkecosystem/crypto";
import { IRoundInfo } from "../../shared";
import { IWalletRepository } from "./repository";
import { IDelegateWallet, IWallet } from "./wallet";

export interface IWalletManager {
    getRepository(): IWalletRepository;

    cloneDelegateWallets(): IWalletManager;

    loadActiveDelegateList(roundInfo: IRoundInfo): IDelegateWallet[];

    buildVoteBalances(): void;

    buildDelegateRanking(delegates: IWallet[], roundInfo?: IRoundInfo): IDelegateWallet[];

    canBePurged(wallet: IWallet): boolean;

    updateVoteBalances(
        sender: IWallet,
        recipient: IWallet,
        transaction: Interfaces.ITransactionData,
        revert?: boolean,
    ): void;

    applyBlock(block: Interfaces.IBlock): void;

    revertBlock(block: Interfaces.IBlock): void;

    applyTransaction(transaction: Interfaces.ITransaction): void;

    revertTransaction(transaction: Interfaces.ITransaction): void;
}
