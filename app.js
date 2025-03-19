// Function to build the API URL with query parameters based on filters
function buildApiUrl() {
  const baseUrl = 'http://127.0.0.1:8000/vehicles/';
  const params = new URLSearchParams();

  // Retrieve filter values
  const partnerLocation = document.getElementById('partnerLocationFilter').value;
  const model = document.getElementById('modelFilter').value;
  const state = document.getElementById('stateFilter').value;
  const exterior = document.getElementById('exteriorFilter').value;
  const interior = document.getElementById('interiorFilter').value;
  const wheels = document.getElementById('wheelsFilter').value;
  const motor = document.getElementById('motorFilter').value;
  const edition = document.getElementById('editionFilter').value;

  // Retrieve price and mileage values
  const minPrice = document.getElementById('minPriceFilter').value;
  const maxPrice = document.getElementById('maxPriceFilter').value;
  const minMileage = document.getElementById('minMileageFilter').value;
  const maxMileage = document.getElementById('maxMileageFilter').value;

  // Retrieve pack selections
  const performancePackCheckbox = document.getElementById('performancePack');
  const pilotPackCheckbox = document.getElementById('pilotPack');
  const plusPackCheckbox = document.getElementById('plusPack');

  const performance = performancePackCheckbox?.checked ? "true" : null;
  const pilot = pilotPackCheckbox?.checked ? "true" : null;
  const plus = plusPackCheckbox?.checked ? "true" : null;

  // Append filter values to URL parameters
  if (partnerLocation) params.append('partner_location', partnerLocation);
  if (model) params.append('model', model);
  if (state) params.append('state', state);
  if (exterior) params.append('exterior', exterior);
  if (interior) params.append('interior', interior);
  if (wheels) params.append('wheels', wheels);
  if (motor) params.append('motor', motor);
  if (edition) params.append('edition', edition);

  if (minPrice) params.append('min_price', minPrice);
  if (maxPrice) params.append('max_price', maxPrice);
  if (minMileage) params.append('min_mileage', minMileage);
  if (maxMileage) params.append('max_mileage', maxMileage);

  if (performance) params.append('performance', performance);
  if (pilot) params.append('pilot', pilot);
  if (plus) params.append('plus', plus);

  console.log("Constructed API URL:", baseUrl + "?" + params.toString());
  return params.toString() ? `${baseUrl}?${params.toString()}` : baseUrl;
}

// Function to fetch vehicles and update the table
function loadVehicles() {
  const url = buildApiUrl();
  fetch(url)
      .then(response => response.json())
      .then(data => {
          console.log("API Response:", data);
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

              // Construct the Polestar vehicle URL
              const vehicleUrl = `https://www.polestar.com/us/preowned-cars/product/polestar-2/${vehicle.id}`;

              // Create a clickable row with an anchor tag inside
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

              // Make the entire row clickable
              row.style.cursor = "pointer";
              row.addEventListener("click", () => {
                  window.open(vehicleUrl, "_blank");
              });

              tbody.appendChild(row);
          });

          // Reattach sorting after updating the table
          attachSortingEvents();
      })
      .catch(error => {
          console.error('Error fetching vehicles:', error);
      });
}


// Function to attach sorting functionality
function attachSortingEvents() {
  const table = document.getElementById('vehiclesTable');
  const headers = table.querySelectorAll('th');

  headers.forEach((header, index) => {
      header.style.cursor = 'pointer';
      header.removeEventListener('click', sortHandler); // Ensure no duplicate listeners
      header.addEventListener('click', sortHandler);
  });
}

// Sort handler function (ensures sorting state is preserved)
function sortHandler(event) {
  const header = event.target.closest('th');
  if (!header) return;

  const table = document.getElementById('vehiclesTable');
  const headers = table.querySelectorAll('th');
  const columnIndex = Array.from(headers).indexOf(header);
  const type = header.getAttribute('data-type') || 'string';

  // Toggle sort order
  const currentOrder = header.getAttribute('data-order') || 'asc';
  const newOrder = currentOrder === 'asc' ? 'desc' : 'asc';
  header.setAttribute('data-order', newOrder);

  // Clear previous arrow indicators
  headers.forEach(h => {
      const arrowSpan = h.querySelector('.sort-arrow');
      if (arrowSpan) arrowSpan.textContent = '';
  });

  // Add arrow indicator
  const arrowSpan = header.querySelector('.sort-arrow');
  if (arrowSpan) {
      arrowSpan.textContent = newOrder === 'asc' ? ' ▲' : ' ▼';
  }

  // Sort the table
  sortTableByColumn(columnIndex, newOrder, type);
}

// Function to sort table columns
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

  // Remove and re-add sorted rows
  while (tbody.firstChild) {
      tbody.removeChild(tbody.firstChild);
  }
  rows.forEach(row => tbody.appendChild(row));
}

// Attach event listeners to all filters including checkboxes
document.querySelectorAll('.form-select, input[type="number"], input[type="checkbox"]').forEach(input => {
  input.addEventListener('input', loadVehicles);
});

// Load vehicles and attach sorting on page load
window.addEventListener('load', () => {
  loadVehicles();
  attachSortingEvents();
});
