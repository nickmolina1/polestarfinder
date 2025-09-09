// --- Globals ---
let fullInventory = null;  // Global variable to hold the full dataset
const VEHICLE_KEYS = [
  "id", "model", "year", "partner_location", "retail_price", "dealer_price", "mileage",
  "first_time_registration", "vin", "stock_images",
  "exterior", "interior", "wheels", "motor", "edition",
  "performance", "pilot", "plus", "state", "available",
  "first_seen_at", "last_seen_at"
];
let defaultSortApplied = false;  // Flag to apply default sort only once

// --- Utility Functions ---
function buildApiUrl() {
  const baseUrl = 'data/vehicles.json'; // Adjust this to your actual API endpoint

  const params = new URLSearchParams();
  // (Assume similar logic as before for dropdowns, number inputs, and checkboxes)
  // ...

  console.log("Constructed API URL:", baseUrl + "?" + params.toString());
  return params.toString() ? `${baseUrl}?${params.toString()}` : baseUrl;
}

function formatPrice(price) {
  if (!price) return ""; // Handle null/undefined cases
  // Remove cents by setting minimumFractionDigits and maximumFractionDigits to 0
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(price);
}

// --- API Functions ---
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
      if (data.vehicles && Array.isArray(data.vehicles)) {
        fullInventory = data.vehicles.map(arr => Object.fromEntries(VEHICLE_KEYS.map((k, i) => [k, arr[i]])));
        localStorage.setItem(cacheKey, JSON.stringify(fullInventory));
      } else {
        fullInventory = [];
        localStorage.setItem(cacheKey, JSON.stringify([]));
      }
      localStorage.setItem(cacheTimestampKey, Date.now().toString());
      updateTable(filterInventory());
    })
    .catch(error => {
      console.error('Error fetching full inventory:', error);
    });
}

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
      if (data.vehicles && Array.isArray(data.vehicles)) {
        fullInventory = data.vehicles.map(arr => Object.fromEntries(VEHICLE_KEYS.map((k, i) => [k, arr[i]])));
        localStorage.setItem(cacheKey, JSON.stringify(fullInventory));
      } else {
        fullInventory = [];
        localStorage.setItem(cacheKey, JSON.stringify([]));
      }
      localStorage.setItem(cacheTimestampKey, Date.now().toString());
      updateTable(filterInventory());
    })
    .catch(error => {
      console.error('Error fetching vehicles:', error);
    });
}

// --- Filtering & Table Update ---
function filterInventory() {
  if (!fullInventory) return [];

  // Get filter values from controls
  const partnerLocation = document.getElementById('partnerLocationFilter').value.toLowerCase();
  const model = document.getElementById('modelFilter').value.toLowerCase();
  const state = document.getElementById('stateFilter').value.toLowerCase();
  const swatches = document.querySelectorAll('#exteriorColorSwatches .color-swatch.selected');
  const exteriorColors = Array.from(swatches).map(el => el.getAttribute('data-color').toLowerCase());
  
  // Use the new interior options (multiple selection)
  const interiorOptions = document.querySelectorAll('#interiorOptions .interior-option.selected');
  const selectedInteriors = Array.from(interiorOptions).map(el => el.getAttribute('data-interior').toLowerCase());
  
  const wheelElements = document.querySelectorAll('#wheelThumbnails a.selected');
  const selectedWheels = Array.from(wheelElements).map(el => el.getAttribute('data-wheel').toLowerCase());
  const motor = document.getElementById('motorFilter').value.toLowerCase();
  const edition = document.getElementById('editionFilter').value.toLowerCase();
  
  const minPrice = parseFloat(document.getElementById('minPriceFilter')?.value) || null;
  const maxPrice = parseFloat(document.getElementById('maxPriceFilter')?.value) || null;
  const minMileage = parseFloat(document.getElementById('minMileageFilter')?.value) || null;
  const maxMileage = parseFloat(document.getElementById('maxMileageFilter')?.value) || null;

  // NEW: Get selected packs from anchors
  const packAnchors = document.querySelectorAll('#packOptions .pack-option.selected');
  const selectedPacks = Array.from(packAnchors).map(el => el.getAttribute('data-pack').toLowerCase());

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
    if (exteriorColors.length > 0 && (!vehicle.exterior || !exteriorColors.includes(vehicle.exterior.toLowerCase())))
      return false;
    if (selectedInteriors.length > 0 && (!vehicle.interior || !selectedInteriors.includes(vehicle.interior.toLowerCase())))
      return false;
    if (selectedWheels.length > 0 && (!vehicle.wheels || !selectedWheels.includes(vehicle.wheels.toLowerCase())))
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
      
    // NEW: Pack filters based on selected anchors.
    if (selectedPacks.length > 0) {
      // For each selected pack, vehicle must have the corresponding flag true.
      if (selectedPacks.includes("performance") && !vehicle.performance)
        return false;
      if (selectedPacks.includes("pilot") && !vehicle.pilot)
        return false;
      if (selectedPacks.includes("plus") && !vehicle.plus)
        return false;
    }

    return true;
  });
}

function updateTable(data) {
  const grid = document.getElementById("vehiclesGrid");
  grid.innerHTML = "";

  if (data.length === 0) {
    grid.innerHTML = '<div class="no-vehicles">No vehicles found</div>';
    return;
  }

  data.forEach((vehicle) => {
    const packs = [];
    if (vehicle.performance) packs.push("Performance");
    if (vehicle.pilot) packs.push("Pilot");
    if (vehicle.plus) packs.push("Plus");
    const packsDisplay = packs.join(", ");
    const modelStr = vehicle.model ? vehicle.model.toLowerCase() : "";
    const vehicleUrl = `https://www.polestar.com/us/preowned-cars/product/polestar-${modelStr.slice(-1)}/${vehicle.id}`;
    const rawPrice = vehicle.retail_price ? parseFloat(vehicle.retail_price) : 0;
    const formattedPrice = rawPrice > 0 ? formatPrice(rawPrice) : "";
    const dateAdded = vehicle.first_seen_at ? new Date(vehicle.first_seen_at).toLocaleDateString("en-US") : "";
    const firstReg = vehicle.first_time_registration ? new Date(vehicle.first_time_registration).toLocaleDateString("en-US") : "";

    const card = document.createElement("div");
    card.className = "vehicle-card";
    card.innerHTML = `
      <div class="card-header">
        <span class="card-model">${vehicle.model || ""}</span>
        <span class="card-year">${vehicle.year || ""}</span>
      </div>
      <div class="card-location">${vehicle.partner_location || ""}</div>
      <div class="card-price">${formattedPrice}</div>
      <div class="card-mileage">${vehicle.mileage ? vehicle.mileage.toLocaleString() + ' mi' : ""}</div>
      <div class="card-state">${vehicle.state || ""}</div>
      <div class="card-packs">${packsDisplay}</div>
      <div class="card-added">Added: ${dateAdded}</div>
      <div class="card-firstreg">First Reg: ${firstReg}</div>
      <a class="card-link" href="${vehicleUrl}" target="_blank">View Details</a>
    `;
    grid.appendChild(card);
  });
}

// --- Sorting Functions ---
function attachSortingEvents() {
  const table = document.getElementById('vehiclesTable');
  const headers = table.querySelectorAll('th');

  headers.forEach((header, index) => {
    header.style.cursor = 'pointer';
    header.removeEventListener('click', sortHandler);
    header.addEventListener('click', sortHandler);
  });
}

function sortTableByColumn(columnIndex, order = "asc", type = "string") {
  const table = document.getElementById("vehiclesTable");
  const tbody = table.querySelector("tbody");
  const rows = Array.from(tbody.querySelectorAll("tr"));

  rows.sort((a, b) => {
    let aText = a.children[columnIndex].textContent.trim();
    let bText = b.children[columnIndex].textContent.trim();
    let aValue = aText;
    let bValue = bText;

    if (type === "number") {
      // Special handling for the price column (assuming it's column index 3)
      if (columnIndex === 3) {
        aValue = parseFloat(a.children[columnIndex].getAttribute("data-price")) || 0;
        bValue = parseFloat(b.children[columnIndex].getAttribute("data-price")) || 0;
      } else {
        aValue = parseFloat(aText.replace(/[^0-9.-]+/g, "")) || 0;
        bValue = parseFloat(bText.replace(/[^0-9.-]+/g, "")) || 0;
      }
    } else {
      aValue = aText.toLowerCase();
      bValue = bText.toLowerCase();
    }

    if (aValue < bValue) {
      return order === "asc" ? -1 : 1;
    }
    if (aValue > bValue) {
      return order === "asc" ? 1 : -1;
    }
    return 0;
  });

  while (tbody.firstChild) {
    tbody.removeChild(tbody.firstChild);
  }
  rows.forEach((row) => tbody.appendChild(row));
}

function sortHandler(event) {
  const header = event.target.closest("th");
  if (!header) return;

  const table = document.getElementById("vehiclesTable");
  const headers = table.querySelectorAll("th");
  const columnIndex = Array.from(headers).indexOf(header);
  const type = header.getAttribute("data-type") || "string";

  const currentOrder = header.getAttribute("data-order") || "asc";
  const newOrder = currentOrder === "asc" ? "desc" : "asc";
  header.setAttribute("data-order", newOrder);

  headers.forEach((h) => {
    const arrowSpan = h.querySelector(".sort-arrow");
    if (arrowSpan) arrowSpan.textContent = "";
  });

  const arrowSpan = header.querySelector(".sort-arrow");
  if (arrowSpan) {
    arrowSpan.textContent = newOrder === "asc" ? " ▲" : " ▼";
  }

  sortTableByColumn(columnIndex, newOrder, type);
}

// --- Event Bindings ---
document.querySelectorAll('.form-select, input[type="number"], input[type="checkbox"]').forEach(input => {
  input.addEventListener('input', () => {
    // On any filter change, update table using cached fullInventory
    updateTable(filterInventory());
  });
});

document.querySelectorAll('#exteriorColorSwatches .color-swatch').forEach(swatch => {
  swatch.addEventListener('click', () => {
    swatch.classList.toggle('selected');
    updateTable(filterInventory());
  });
});

document.querySelectorAll('#wheelThumbnails a').forEach(anchor => {
  anchor.addEventListener('click', function(e) {
    e.preventDefault();
    this.classList.toggle('selected');
    updateTable(filterInventory());
  });
});

document.querySelectorAll('#interiorOptions .interior-option').forEach(option => {
  option.addEventListener('click', function(e) {
    e.preventDefault();
    this.classList.toggle('selected');
    updateTable(filterInventory());
  });
});


document.querySelectorAll('#packOptions .pack-option').forEach(option => {
    option.addEventListener('click', function(e) {
      e.preventDefault();
      this.classList.toggle('selected');
      updateTable(filterInventory());
    });
  });


// --- Initialization ---
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
