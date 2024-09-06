from hist import Hist
from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import numpy as np

class TopQuarkProcessor(processor.ProcessorABC):
    def __init__(self):
        # Define histograms for top quark identification
        self._accumulator = processor.dict_accumulator({
            'top_mass': Hist.new.Reg(50, 100, 300, name="top_mass", label="Reconstructed Top Quark Mass [GeV]").Double(),
            'n_bjets': Hist.new.Reg(5, 0, 5, name="n_bjets", label="Number of b-jets").Double(),
            'lepton_pt': Hist.new.Reg(50, 0, 500, name="lepton_pt", label="Lepton pT [GeV]").Double()
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        dataset = events.metadata['dataset']
        jets = events.Jet
        electrons = events.Electron
        muons = events.Muon

        # Select events with at least one lepton (electron or muon)
        lepton_mask = (electrons.pt > 20) & (abs(electrons.eta) < 2.5)
        lepton_mask = lepton_mask | ((muons.pt > 20) & (abs(muons.eta) < 2.4))

        # Select events with at least one b-tagged jet
        bjets = jets[(jets.btagDeepFlavB > 0.2770) & (jets.pt > 30) & (abs(jets.eta) < 2.4)]
        bjet_mask = (bjets.counts > 0)

        # Combine masks for events with at least one lepton and one b-jet
        selected_events = events[lepton_mask & bjet_mask]

        # Reconstruct top quark candidate mass
        top_candidates = selected_events.Jet[:, 0] + selected_events.Jet[:, 1] + selected_events.MET
        top_mass = top_candidates.mass

        # Fill histograms
        output = self.accumulator.identity()
        output['top_mass'].fill(top_mass=top_mass)
        output['n_bjets'].fill(n_bjets=bjets.counts)
        output['lepton_pt'].fill(lepton_pt=selected_events.Electron.pt.flatten() if len(selected_events.Electron) > 0 else selected_events.Muon.pt.flatten())
        
        return output

    def postprocess(self, accumulator):
        return accumulator

# Run the processor on a NanoAOD file
#file = "root://cmsxrootd.fnal.gov//store/mc/RunIISummer16NanoAODv7/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v13-v1/280000/FF3D84A5-9A2D-E94C-9B8D-A419A3C3B9E0.root"
file = "root://eos/uscms/store/mc/RunIISummer20UL16NanoAOD/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v13-v1/00000/107E41B2-1B80-0140-81B1-31BCD834D5BD.root"

events = NanoEventsFactory.from_root(file, schemaclass=NanoAODSchema).events()

processor_instance = TopQuarkProcessor()
output = processor.run_uproot_job(
    {'sample': [file]},
    treename='Events',
    processor_instance=processor_instance,
    executor=processor.iterative_executor,
    executor_args={'schema': NanoAODSchema, 'workers': 4},
)

print(output)

