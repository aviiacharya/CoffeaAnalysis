import awkward as ak
from coffea import processor
from hist import Hist
from coffea.nanoevents import NanoAODSchema
import matplotlib.pyplot as plt

class TopQuarkProcessor(processor.ProcessorABC):
    def __init__(self):
        # Define histograms to accumulate hadronic and leptonic top quark masses
        self._accumulator = processor.dict_accumulator({
            'hadronic_top_mass': hist.Hist(
                "Events", hist.Bin("mass", "Hadronic Top Mass [GeV]", 50, 100, 300)
            ),
            'leptonic_top_mass': hist.Hist(
                "Events", hist.Bin("mass", "Leptonic Top Mass [GeV]", 50, 100, 300)
            )
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        # Extract leptons (electrons and muons)
        electrons = events.Electron
        muons = events.Muon

        # Lepton pt selection
        electron_pt = electrons.pt
        muon_pt = muons.pt

        # Select GenParticles for top quarks
        genparticles = events.GenPart
        tops = genparticles[(abs(genparticles.pdgId) == 6)]  # PDG ID 6 is the top quark
        top_pt = tops.pt

        # Children of top quarks 
        tops_children = tops.distinctChildren

        # Identify leptonic decays 
        is_leptonic = ak.any(abs(tops_children.pdgId) == 11, axis=-1) | \
                      ak.any(abs(tops_children.pdgId) == 13, axis=-1) | \
                      ak.any(abs(tops_children.pdgId) == 15, axis=-1)

        # Hadronic decay
        is_hadronic = ~is_leptonic

        # Get the mass of the top quarks
        hadronic_top_mass = tops[is_hadronic].mass
        leptonic_top_mass = tops[is_leptonic].mass

        # Fill histograms with the mass of top quarks from hadronic and leptonic decays
        dataset_name = events.metadata['dataset']
        self.accumulator['hadronic_top_mass'].fill(mass=hadronic_top_mass)
        self.accumulator['leptonic_top_mass'].fill(mass=leptonic_top_mass)

        return self.accumulator

    def postprocess(self, accumulator):
        pass


def run_analysis():
    from coffea.processor import run
    from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

    fname = "root://cmsxrootd.fnal.gov//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/0520A050-AF68-EF43-AA5B-5AA77C74ED73.root"

    # Create the fileset
    fileset = {'TTToSemiLeptonic': [fname]}
    
    result = run(
        fileset=fileset,
        treename='Events',
        processor_instance=TopQuarkProcessor(),
        executor=processor.futures_executor,
        executor_args={'schema': NanoAODSchema, 'workers': 4},
        chunksize=100000
    )

    return result


if __name__ == "__main__":

    result = run_analysis()

 
    hadronic_hist = result['hadronic_top_mass']
    leptonic_hist = result['leptonic_top_mass']
    plt.figure(figsize=(10, 6))
    hist.plot1d(hadronic_hist, clear=False, label="Hadronic Top Decay", color="blue")
    hist.plot1d(leptonic_hist, clear=False, label="Leptonic Top Decay", color="red")

    plt.title("Top Quark Mass Distribution")
    plt.xlabel("Mass [GeV]")
    plt.ylabel("Events")
    plt.legend()
    plt.grid(True)

    plt.savefig("top_quark_mass_distribution.png")

    plt.show()

