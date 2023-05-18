import { displayError } from './error.js';
import {displayMoleculeCard} from "./molecule-card.js";

const optionsForm = document.getElementById('search-options');
const openButton = document.getElementById('options-dropdown-open');
const closeButton = document.getElementById('options-dropdown-close');

function showOptions() {
    optionsForm.classList.add('search-options-open');

    if (optionsForm.className.includes('search-options-closed')) {
        optionsForm.classList.remove('search-options-closed');
    }

    openButton.classList.add('hidden');
    closeButton.classList.remove('hidden');
}

function hideOptions() {
    optionsForm.classList.add('search-options-closed');

    if (optionsForm.className.includes('search-options-open')) {
        optionsForm.classList.remove('search-options-open');
    }

    closeButton.classList.add('hidden');
    openButton.classList.remove('hidden');
}

openButton.addEventListener('click', showOptions);
closeButton.addEventListener('click', hideOptions);

function addModelNameOption(name) {
    let wrapper = document.createElement('div');
    wrapper.className = 'custom-control custom-radio';
    wrapper.style.border = "30px"


    let checkbox = document.createElement('input');
    checkbox.type = 'radio';
    checkbox.checked = false;
    checkbox.name = "databases";
    checkbox.id = name;
    checkbox.classList.add('custom-control-input');

    let label = document.createElement('label');
    label.innerText = name;
    label.htmlFor = name;
    label.classList.add('custom-control-label');
    label.style.fontSize = '16px'

    let buffer = document.createElement('p');

    wrapper.append(checkbox);
    wrapper.append(label);
    wrapper.append(buffer);
    optionsForm.append(wrapper);
}

export function displayModelNameOptions(propNames) {
    for (const propName of propNames) {
        addModelNameOption(propName);
    }
}

export function getOptions() {
    const optionsFormData = new FormData(optionsForm);
    let options = {};

    for (const [key, value] of optionsFormData.entries()) {
        // Ticked checkboxes have a value of 'on', need to change it to true
        options[key] = value == 'on' ? true : value;
    }

    return options;
}

fetch('/models', {
    method: 'GET',
})
    .then((response) =>
        response.ok ? response.json() : Promise.reject(response)
    )
    .then((data) => displayModelNameOptions(data))
    .catch((err) => displayError(err));

