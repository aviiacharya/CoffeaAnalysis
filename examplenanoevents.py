import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import matplotlib.pyplot as plt

NanoAODSchema.warn_missing_crossrefs = False

#fname = "https://raw.githubusercontent.com/CoffeaTeam/coffea/master/tests/samples/nano_dy.root"
fname = "root://cmsxrootd.fnal.gov//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/0520A050-AF68-EF43-AA5B-5AA77C74ED73.root"

events = NanoEventsFactory.from_root(
    {fname: "Events"}, # We pass a dictionary of {filename1: treename1, filename2: treename2, ...} to load from
    schemaclass=NanoAODSchema.v6,
    metadata={"dataset": "DYJets"},
    delayed=False, # You can turn this to True and insert `` commands at the end of variables
).events()

# Access the 'id1' property from the Generator information
id1_values = events.Generator.id1

id1_fields = events.GenJet.fields

id1_energy = events.GenJet.energy
# Print the output
print("Generator id1 values:",id1_values)
print("Generator id1 fields:",id1_fields)
print("Generator id1 energy:",id1_energy)

# Ensure that Jet and Electron collections exist in the file
if hasattr(events, 'Jet') and hasattr(events, 'Electron'):
    # Calculate deltaR between the leading jet and all electrons in each event
    dr = events.Jet[:, 0].delta_r(events.Electron)

    # Print the output for all distances
    print("Distance (deltaR) between the leading jet and all electrons:")
    print(dr)

    # Find the minimum distance (deltaR) for each event
    min_dr = ak.min(dr, axis=1)

    # Print the minimum distances
    print("Minimum distance (deltaR) between the leading jet and any electron in each event:")
    print(min_dr)
else:
    print("Jet or Electron collection not found in the input file.")

# Ensure that Electron collection exists in the file
if hasattr(events, 'Electron'):
    # Select electrons based on some criteria (e.g., pt > 10 GeV, |eta| < 2.5)
    selected_electrons = events.Electron[(events.Electron.pt > 10) & (abs(events.Electron.eta) < 2.5)]

    # Flatten the mass array to handle variable-length subarrays
    electron_mass_flat = ak.flatten(selected_electrons.mass)
    
    # Convert to NumPy array for mass plotting
    electron_mass_np = ak.to_numpy(electron_mass_flat)
    
    # Plot the mass distribution of electrons
    plt.figure(figsize=(8, 6))
    plt.hist(electron_mass_np, bins=50, color='blue', alpha=0.7, label='Electron Mass')
    plt.xlabel('Mass (GeV)')
    plt.ylabel('Frequency')
    plt.title('Electron Mass Distribution')
    plt.legend()
    
    # Save the plot as a PNG file
    plt.savefig('electron_mass_distribution.png')  # Save as PNG
    plt.show()

else:
    print("Electron collection not found in the input file.")
