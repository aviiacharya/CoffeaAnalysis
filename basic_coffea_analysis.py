from hist import Hist
from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import numpy as np

# Define a simple processor class
class SimpleProcessor(processor.ProcessorABC):
    def __init__(self):
        # Define a histogram for the invariant mass of two leptons
        self._accumulator = processor.dict_accumulator({
            'mass': hist.Hist(
                'Events',
                hist.Cat('dataset', 'Dataset'),
                hist.Bin('mass', 'Invariant Mass [GeV]', 50, 0, 200),
            )
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        dataset = events.metadata['dataset']
        electrons = events.Electron

        # Select events with exactly two electrons
        selection = (electrons.counts == 2)
        dielectrons = electrons[selection]

        # Calculate invariant mass of dielectron system
        mass = (dielectrons[:, 0] + dielectrons[:, 1]).mass

        # Fill histogram
        output = self.accumulator.identity()
        output['mass'].fill(dataset=dataset, mass=mass)
        return output

    def postprocess(self, accumulator):
        return accumulator

# Run the processor on a NanoAOD file
file = "root://cmsxrootd.fnal.gov//store/mc/RunIISummer16NanoAODv7/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v13-v1/280000/FF3D84A5-9A2D-E94C-9B8D-A419A3C3B9E0.root"
events = NanoEventsFactory.from_root(file, schemaclass=NanoAODSchema).events()

processor_instance = SimpleProcessor()
output = processor.run_uproot_job(
    {'sample': [file]},
    treename='Events',
    processor_instance=processor_instance,
    executor=processor.iterative_executor,
    executor_args={'schema': NanoAODSchema, 'workers': 4},
)

print(output)

