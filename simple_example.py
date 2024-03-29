import time
from distributed import Client
from gsdcjobqueue import GSDCCondorCluster
from coffea import hist, processor, nanoevents
import awkward as ak


class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        self._accumulator = processor.dict_accumulator(
            {
                "sumw": processor.defaultdict_accumulator(float),
                "mass": hist.Hist(
                    "Events",
                    hist.Cat("dataset", "Dataset"),
                    hist.Bin("mass", r"$m_{\mu\mu}$ [GeV]", 60, 60, 120),
                ),
            }
        )

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator.identity()

        dataset = events.metadata["dataset"]
        muons = events.Muon

        cut = (ak.num(muons) == 2) & (ak.sum(muons.charge, axis=-1) == 0)
        # add first and second muon in every event together
        dimuon = muons[cut][:, 0] + muons[cut][:, 1]

        output["sumw"][dataset] += len(events)
        output["mass"].fill(
            dataset=dataset,
            mass=dimuon.mass,
        )

        return output

    def postprocess(self, accumulator):
        return accumulator


if __name__ == "__main__":
    tic = time.time()
    cluster = GSDCCondorCluster()
    # minimum > 0: https://github.com/CoffeaTeam/coffea/issues/465
    cluster.adapt(minimum=1, maximum=10)
    client = Client(cluster)

    exe_args = {
        "client": client,
        "savemetrics": True,
        "schema": nanoevents.NanoAODSchema,
        "align_clusters": True,
    }

    proc = MyProcessor()

    print("Waiting for at least one worker...")
    client.wait_for_workers(1)
    hists, metrics = processor.run_uproot_job(
        "fileset.json",
        treename="Events",
        processor_instance=proc,
        executor=processor.dask_executor,
        executor_args=exe_args,
        # remove this to run on the whole fileset:
        maxchunks=10,
    )

    elapsed = time.time() - tic
    print(f"Output: {hists}")
    print(f"Metrics: {metrics}")
    print(f"Finished in {elapsed:.1f}s")
    print(f"Events/s: {metrics['entries'] / elapsed:.0f}")
