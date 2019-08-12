import { app } from "@arkecosystem/core-container";
import { ApplicationEvents } from "@arkecosystem/core-event-emitter";
import { EventEmitter } from "@arkecosystem/core-interfaces";
import { Interfaces, Managers } from "@arkecosystem/crypto";
import { ModuleThread, spawn, Worker } from "threads"
import { IPendingTransactionJobResult, ITransactionWorkerJob } from './types';
import { WorkerApi } from './worker';

export type FinishedJobCallback = (job: IPendingTransactionJobResult) => void;

export class PoolBroker {
    private worker: ModuleThread<WorkerApi>;

    public constructor(private readonly finishedJobCallback: FinishedJobCallback) {
        app.resolvePlugin<EventEmitter.EventEmitter>("event-emitter").on(
            ApplicationEvents.BlockApplied,
            (data: Interfaces.IBlockData) => {
                this.worker.updateBlockHeight(data.height);
            },
        );
    }

    public async init(): Promise<void> {
        this.worker = await spawn<WorkerApi>(new Worker("./worker.js"))

        // @ts-ignore
        this.worker.getObservable().subscribe((pendingJob: IPendingTransactionJobResult) => {
            this.finishedJobCallback(pendingJob);
        });

        const options: Record<string, any> = ({
            ...app.resolveOptions("transaction-pool"),
            networkName: Managers.configManager.get("network.name"),
            lastHeight: Managers.configManager.getHeight(),
        });

        await this.worker.configure(options);
    }

    public async createJob(job: ITransactionWorkerJob): Promise<void> {
        return this.worker.createJob(job);
    }
}
