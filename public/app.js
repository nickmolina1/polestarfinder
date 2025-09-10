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

// Image angle to use when displaying vehicle images
const ANGLE = 0;


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

// Return the first image URL from various possible shapes for stock_images
function extractFirstImage(images) {
  if (!images) return null;
  let candidate = null;

  // Already an array
  if (Array.isArray(images) && images.length) {
    candidate = images[0];
  } else if (typeof images === 'string') {
    // JSON string or plain URL
    try {
      const parsed = JSON.parse(images);
      if (Array.isArray(parsed) && parsed.length) candidate = parsed[0];
      else candidate = parsed;
    } catch (e) {
      candidate = images;
    }
  } else if (typeof images === 'object') {
    // Plain object with numeric keys or first value
    for (const k in images) {
      if (images[k]) {
        candidate = images[k];
        break;
      }
    }
  }

  if (!candidate) return null;

  // If candidate is an object, try to pull the first string value (e.g., url)
  if (typeof candidate === 'object') {
    for (const k in candidate) {
      if (typeof candidate[k] === 'string') {
        candidate = candidate[k];
        break;
      }
    }
  }

  // If it's a string, replace angle=0 with angle=1
  if (typeof candidate === 'string') {
    return candidate.replace(/angle=0\b/g, `angle=${ANGLE}`);
  }

  return null;
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
  renderCurrentView();
    return;
  }

  const url = buildApiUrl();  // For full inventory, you might want no extra filters.
  fetch(url)
    .then(response => response.json())
    .then(data => {
      console.log("Fetched full inventory:", data);
      if (data.vehicles && Array.isArray(data.vehicles)) {
        fullInventory = data.vehicles.map(arr => {
          const obj = Object.fromEntries(VEHICLE_KEYS.map((k, i) => [k, arr[i]]));
          if (obj.partner_location && obj.partner_location.startsWith('Polestar ')) {
            obj.partner_location = obj.partner_location.replace(/^Polestar\s+/i, '').trim();
          }
          return obj;
        });
        localStorage.setItem(cacheKey, JSON.stringify(fullInventory));
      } else {
        fullInventory = [];
        localStorage.setItem(cacheKey, JSON.stringify([]));
      }
      localStorage.setItem(cacheTimestampKey, Date.now().toString());
  renderCurrentView();
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
  renderCurrentView();
    return;
  }

  fetch(url)
    .then(response => response.json())
    .then(data => {
      console.log("API Response:", data);
      if (data.vehicles && Array.isArray(data.vehicles)) {
        fullInventory = data.vehicles.map(arr => {
          const obj = Object.fromEntries(VEHICLE_KEYS.map((k, i) => [k, arr[i]]));
          if (obj.partner_location && obj.partner_location.startsWith('Polestar ')) {
            obj.partner_location = obj.partner_location.replace(/^Polestar\s+/i, '').trim();
          }
          return obj;
        });
        localStorage.setItem(cacheKey, JSON.stringify(fullInventory));
      } else {
        fullInventory = [];
        localStorage.setItem(cacheKey, JSON.stringify([]));
      }
      localStorage.setItem(cacheTimestampKey, Date.now().toString());
  renderCurrentView();
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
  const swatches = document.querySelectorAll('#exteriorOptions .exterior-option.selected');
  const exteriorColors = Array.from(swatches).map(el => el.getAttribute('data-color').toLowerCase());
  
  // Use the new interior options (multiple selection)
  const interiorOptions = document.querySelectorAll('#interiorOptions .interior-option.selected');
  const selectedInteriors = Array.from(interiorOptions).map(el => el.getAttribute('data-interior').toLowerCase());
  
  const wheelElements = document.querySelectorAll('#wheelOptions a.selected');
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
    const newLocal = extractFirstImage(vehicle.stock_images);
    const firstImage = newLocal;
    const imageHtml = firstImage ? `<img class="card-image" src="${firstImage}" alt="${(vehicle.model||'').replace(/"/g,'')}">` : `<div class="card-image placeholder"></div>`;

    card.innerHTML = `
      <div class="card-header">
        <span class="card-model">${vehicle.model || ""}</span>
        <span class="card-year">${vehicle.year || ""}</span>
      </div>
      ${imageHtml}
  <div class="card-location">${vehicle.partner_location || ""}</div>
  <div class="card-price">${formattedPrice}</div>
  <div class="card-mileage">${vehicle.mileage ? vehicle.mileage.toLocaleString() + ' mi' : ""}</div>
  <div class="card-state">${vehicle.state || ""}</div>
  <div class="card-packs">${packsDisplay}</div>
    `;
    // Make the whole card act as a link (click and keyboard accessible)
    const ariaLabel = `Open details for ${(vehicle.model||'').trim()} ${(vehicle.year||'').toString()}`;
    card.setAttribute('tabindex', '0');
    card.setAttribute('role', 'link');
    card.setAttribute('aria-label', ariaLabel);
    card.addEventListener('click', () => {
      window.open(vehicleUrl, '_blank');
    });
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        window.open(vehicleUrl, '_blank');
      }
    });
    grid.appendChild(card);
  });

  // Update statistics for the currently displayed set
  updateStats(data);
}

// Update the statistics panel (total count, average retail price, average mileage)
function updateStats(data) {
  const totalEl = document.getElementById('totalVehicles');
  const avgPriceEl = document.getElementById('averageRetailPrice');
  const avgMileageEl = document.getElementById('averageMileage');
  if (!totalEl || !avgPriceEl || !avgMileageEl) return;

  const total = data.length;
  let priceSum = 0;
  let priceCount = 0;
  let mileageSum = 0;
  let mileageCount = 0;

  data.forEach(v => {
    const p = parseFloat(v.retail_price);
    if (!isNaN(p) && p > 0) { priceSum += p; priceCount++; }
    const m = parseFloat(v.mileage);
    if (!isNaN(m) && m >= 0) { mileageSum += m; mileageCount++; }
  });

  const avgPrice = priceCount ? (priceSum / priceCount) : 0;
  const avgMileage = mileageCount ? Math.round(mileageSum / mileageCount) : 0;

  totalEl.textContent = total.toString();
  avgPriceEl.textContent = avgPrice ? formatPrice(avgPrice) : '$0';
  avgMileageEl.textContent = avgMileage ? avgMileage.toLocaleString() : '0';
}

// Helper to compute filtered + sorted data and render
function renderCurrentView() {
  const filtered = filterInventory();
  const sorted = applySorting(filtered);
  updateTable(sorted);
}

// Sorting support: read controls and sort data accordingly
function applySorting(data) {
  const sortField = document.getElementById('sortField')?.value;
  const sortOrder = document.getElementById('sortOrder')?.value || 'asc';
  if (!sortField) return data;

  const sorted = [...data].sort((a, b) => {
    let aVal = a[sortField];
    let bVal = b[sortField];
    // coerce to numbers where applicable
    const numFields = ['retail_price', 'mileage', 'year'];
    if (numFields.includes(sortField)) {
      aVal = parseFloat(aVal) || 0;
      bVal = parseFloat(bVal) || 0;
    } else {
      aVal = (aVal || '').toString().toLowerCase();
      bVal = (bVal || '').toString().toLowerCase();
    }

    if (aVal < bVal) return sortOrder === 'asc' ? -1 : 1;
    if (aVal > bVal) return sortOrder === 'asc' ? 1 : -1;
    return 0;
  });
  return sorted;
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
  renderCurrentView();
  });
});

// Helper to wire up selectable thumbnail-like elements consistently
function setupSelectable(containerSelector, itemSelector) {
  const container = document.querySelector(containerSelector);
  if (!container) return;
  container.querySelectorAll(itemSelector).forEach(el => {
    // ensure keyboard focusability
    if (!el.hasAttribute('tabindex')) el.setAttribute('tabindex', '0');

    el.addEventListener('click', (e) => {
      // prevent anchor navigation where applicable
      if (el.tagName === 'A') e.preventDefault();
      el.classList.toggle('selected');
      renderCurrentView();
    });

    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        if (el.tagName === 'A') {
          // don't navigate away
        }
        el.classList.toggle('selected');
        renderCurrentView();
      }
    });
  });
}

// Apply to the various thumbnail groups
setupSelectable('#exteriorOptions', '.exterior-option');
setupSelectable('#interiorOptions', '.interior-option');
setupSelectable('#wheelOptions', 'a');
setupSelectable('#packOptions', '.pack-option');



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
    // Wire sort controls
    const sortField = document.getElementById('sortField');
    const sortOrder = document.getElementById('sortOrder');
    if (sortField && sortOrder) {
  sortField.addEventListener('change', () => renderCurrentView());
  sortOrder.addEventListener('change', () => renderCurrentView());
    }
  });
