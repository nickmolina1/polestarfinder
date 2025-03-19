let fullInventory = null;  // Global variable to hold the full dataset

// Function to build the API URL with query parameters based on filters (for initial fetch)
function buildApiUrl() {
  const baseUrl = 'http://127.0.0.1:8000/vehicles/';
  const params = new URLSearchParams();
  // (Assume similar logic as before for dropdowns, number inputs, and checkboxes)
  // ...
  console.log("Constructed API URL:", baseUrl + "?" + params.toString());
  return params.toString() ? `${baseUrl}?${params.toString()}` : baseUrl;
}

// Function to fetch full inventory and cache it
function fetchFullInventory() {
  const cacheKey = "fullVehicleInventory";
  const cacheTimestampKey = "fullVehicleInventoryTimestamp";
  const oneHour = 3600000; // 1 hour in milliseconds

  const cachedData = localStorage.getItem(cacheKey);
  const cachedTimestamp = localStorage.getItem(cacheTimestampKey);

  if (cachedData && cachedTimestamp && (Date.now() - parseInt(cachedTimestamp, 10)) < oneHour) {
    console.log("Using cached full inventory");
    fullInventory = JSON.parse(cachedData);
    updateTable(filterInventory());
    return;
  }

  const url = buildApiUrl();  // For full inventory, you might want no extra filters.
  fetch(url)
    .then(response => response.json())
    .then(data => {
      console.log("Fetched full inventory:", data);
      fullInventory = data;  // Cache in global variable
      localStorage.setItem(cacheKey, JSON.stringify(data));
      localStorage.setItem(cacheTimestampKey, Date.now().toString());
      updateTable(filterInventory());
    })
    .catch(error => {
      console.error('Error fetching full inventory:', error);
    });
}

// Function to filter the fullInventory based on current controls
function filterInventory() {
  if (!fullInventory) return [];

  // Get filter values from controls
  const partnerLocation = document.getElementById('partnerLocationFilter').value.toLowerCase();
  const model = document.getElementById('modelFilter').value.toLowerCase();
  const state = document.getElementById('stateFilter').value.toLowerCase();
  const exterior = document.getElementById('exteriorFilter').value.toLowerCase();
  const interior = document.getElementById('interiorFilter').value.toLowerCase();
  const wheels = document.getElementById('wheelsFilter').value.toLowerCase();
  const motor = document.getElementById('motorFilter').value.toLowerCase();
  const edition = document.getElementById('editionFilter').value.toLowerCase();
  
  const minPrice = parseFloat(document.getElementById('minPriceFilter')?.value) || null;
  const maxPrice = parseFloat(document.getElementById('maxPriceFilter')?.value) || null;
  const minMileage = parseFloat(document.getElementById('minMileageFilter')?.value) || null;
  const maxMileage = parseFloat(document.getElementById('maxMileageFilter')?.value) || null;

  const performance = document.getElementById('performancePack').checked;
  const pilot = document.getElementById('pilotPack').checked;
  const plus = document.getElementById('plusPack').checked;

  // Filter the fullInventory array
  return fullInventory.filter(vehicle => {
    // Check each filter; if control is empty/unchecked, ignore it.
    // Use case-insensitive comparison for text fields.
    if (partnerLocation && (!vehicle.partner_location || vehicle.partner_location.toLowerCase() !== partnerLocation))
      return false;
    if (model && (!vehicle.model || vehicle.model.toLowerCase() !== model))
      return false;
    if (state && (!vehicle.state || vehicle.state.toLowerCase() !== state))
      return false;
    if (exterior && (!vehicle.exterior || vehicle.exterior.toLowerCase() !== exterior))
      return false;
    if (interior && (!vehicle.interior || vehicle.interior.toLowerCase() !== interior))
      return false;
    if (wheels && (!vehicle.wheels || vehicle.wheels.toLowerCase() !== wheels))
      return false;
    if (motor && (!vehicle.motor || vehicle.motor.toLowerCase() !== motor))
      return false;
    if (edition && (!vehicle.edition || vehicle.edition.toLowerCase() !== edition))
      return false;
    if (minPrice !== null && (!vehicle.retail_price || parseFloat(vehicle.retail_price) < minPrice))
      return false;
    if (maxPrice !== null && (!vehicle.retail_price || parseFloat(vehicle.retail_price) > maxPrice))
      return false;
    if (minMileage !== null && (!vehicle.mileage || parseFloat(vehicle.mileage) < minMileage))
      return false;
    if (maxMileage !== null && (!vehicle.mileage || parseFloat(vehicle.mileage) > maxMileage))
      return false;
    
    // For pack checkboxes, assume if unchecked, don't filter; if checked, vehicle must have that pack true.
    if (performance && !vehicle.performance)
      return false;
    if (pilot && !vehicle.pilot)
      return false;
    if (plus && !vehicle.plus)
      return false;

    return true;
  });
}

// Function to update the table with vehicle data
function updateTable(data) {
  const tbody = document.querySelector('#vehiclesTable tbody');
  tbody.innerHTML = ''; // Clear existing table rows

  if (data.length === 0) {
    tbody.innerHTML = '<tr><td colspan="12" class="text-center">No vehicles found</td></tr>';
    return;
  }

  data.forEach(vehicle => {
    const packs = [];
    if (vehicle.performance) packs.push("Performance");
    if (vehicle.pilot) packs.push("Pilot");
    if (vehicle.plus) packs.push("Plus");
    const packsDisplay = packs.join(", ");
    const vehicleUrl = `https://www.polestar.com/us/preowned-cars/product/polestar-${vehicle.model.toLowerCase().slice(-1)}/${vehicle.id}`;

    const row = document.createElement('tr');
    row.innerHTML = `
      <td><a href="${vehicleUrl}" target="_blank">${vehicle.model}</a></td>
      <td>${vehicle.year}</td>
      <td>${vehicle.partner_location || ''}</td>
      <td>${vehicle.retail_price || ''}</td>
      <td>${vehicle.vin || ''}</td>
      <td>${vehicle.exterior || ''}</td>
      <td>${vehicle.interior || ''}</td>
      <td>${vehicle.wheels || ''}</td>
      <td>${vehicle.motor || ''}</td>
      <td>${vehicle.state || ''}</td>
      <td>${vehicle.edition || ''}</td>
      <td>${packsDisplay}</td>
    `;
    row.style.cursor = "pointer";
    row.addEventListener("click", () => {
      window.open(vehicleUrl, "_blank");
    });
    tbody.appendChild(row);
  });
  attachSortingEvents();
}

// Modified loadVehicles() to use caching and client-side filtering
function loadVehicles() {
  const cacheKey = "fullVehicleInventory";
  const cacheTimestampKey = "fullVehicleInventoryTimestamp";
  const oneHour = 3600000; // 1 hour in milliseconds

  const url = buildApiUrl(); // This URL is only used for the initial fetch.
  const cachedData = localStorage.getItem(cacheKey);
  const cachedTimestamp = localStorage.getItem(cacheTimestampKey);

  if (cachedData && cachedTimestamp && (Date.now() - parseInt(cachedTimestamp, 10)) < oneHour) {
    console.log("Using cached full inventory");
    fullInventory = JSON.parse(cachedData);
    updateTable(filterInventory());
    return;
  }

  fetch(url)
    .then(response => response.json())
    .then(data => {
      console.log("API Response:", data);
      fullInventory = data;
      localStorage.setItem(cacheKey, JSON.stringify(data));
      localStorage.setItem(cacheTimestampKey, Date.now().toString());
      updateTable(filterInventory());
    })
    .catch(error => {
      console.error('Error fetching vehicles:', error);
    });
}

// --- SORTING FUNCTIONALITY WITH ARROW INDICATORS ---
function attachSortingEvents() {
  const table = document.getElementById('vehiclesTable');
  const headers = table.querySelectorAll('th');

  headers.forEach((header, index) => {
    header.style.cursor = 'pointer';
    header.removeEventListener('click', sortHandler);
    header.addEventListener('click', sortHandler);
  });
}

function sortHandler(event) {
  const header = event.target.closest('th');
  if (!header) return;
  const table = document.getElementById('vehiclesTable');
  const headers = table.querySelectorAll('th');
  const columnIndex = Array.from(headers).indexOf(header);
  const type = header.getAttribute('data-type') || 'string';
  const currentOrder = header.getAttribute('data-order') || 'asc';
  const newOrder = currentOrder === 'asc' ? 'desc' : 'asc';
  header.setAttribute('data-order', newOrder);

  headers.forEach(h => {
    const arrowSpan = h.querySelector('.sort-arrow');
    if (arrowSpan) arrowSpan.textContent = '';
  });

  const arrowSpan = header.querySelector('.sort-arrow');
  if (arrowSpan) {
    arrowSpan.textContent = newOrder === 'asc' ? ' ▲' : ' ▼';
  }

  sortTableByColumn(columnIndex, newOrder, type);
}

function sortTableByColumn(columnIndex, order = 'asc', type = 'string') {
  const table = document.getElementById('vehiclesTable');
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));

  rows.sort((a, b) => {
    const aText = a.children[columnIndex].textContent.trim();
    const bText = b.children[columnIndex].textContent.trim();
    let aValue = aText;
    let bValue = bText;

    if (type === 'number') {
      aValue = parseFloat(aText) || 0;
      bValue = parseFloat(bText) || 0;
    } else {
      aValue = aText.toLowerCase();
      bValue = bText.toLowerCase();
    }

    if (aValue < bValue) {
      return order === 'asc' ? -1 : 1;
    }
    if (aValue > bValue) {
      return order === 'asc' ? 1 : -1;
    }
    return 0;
  });

  while (tbody.firstChild) {
    tbody.removeChild(tbody.firstChild);
  }
  rows.forEach(row => tbody.appendChild(row));
}

// Attach event listeners to all filter controls
document.querySelectorAll('.form-select, input[type="number"], input[type="checkbox"]').forEach(input => {
  input.addEventListener('input', () => {
    // On any filter change, update table using cached fullInventory
    updateTable(filterInventory());
  });
});

// Load vehicles on page load
window.addEventListener('load', () => {
    // Check if this navigation was a page reload
    const navEntries = performance.getEntriesByType("navigation");
    if (navEntries.length > 0 && navEntries[0].type === "reload") {
      localStorage.removeItem("fullVehicleInventory");
      localStorage.removeItem("fullVehicleInventoryTimestamp");
      console.log("Cache cleared due to page reload.");
    }
    loadVehicles();
  });
  