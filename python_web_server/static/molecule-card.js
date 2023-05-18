const moleculeWrapper = document.querySelector('.res-wrapper');
const moleculeProperties = document.querySelector('.molecule-properties');
const moleculeSVG = document.getElementById('molecule-svg');
const moleculeSMILES = document.getElementById('molecule_smile_string');
const originDist = document.getElementById('origin-dist');
const loadingWrapper = document.querySelector('.loading-wrapper');

export function showMoleculeWrapper() {
    if (moleculeWrapper.className.includes('hidden')) {
        moleculeWrapper.classList.remove('hidden');
    }
}

export function hideMoleculeWrapper() {
    if (!moleculeWrapper.className.includes('hidden')) {
        moleculeWrapper.classList.add('hidden');
    }
}

export function hideLoadingWrapper() {
    if (!loadingWrapper.className.includes('hidden')) {
        loadingWrapper.classList.add('hidden');
    }
}

export function displayMoleculeCard(moleculeData) {
    hideLoadingWrapper();
    showMoleculeWrapper();

    moleculeProperties.innerHTML = moleculeData.grid_html;
    moleculeSMILES.innerHTML = moleculeData.SMILES;
    originDist.innerHTML = moleculeData.o_dist;
    moleculeSVG.innerHTML = moleculeData.svg;
}