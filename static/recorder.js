let mediaRecorder;
let audioChunks = [];
let audioBlob;
let sessionData = {};

// Add near the top after variable declarations
const SESSION_STORAGE_KEY = 'whisperSessionData';
const CURRENT_ROW_KEY = 'whisperCurrentRow';

// Add near the top with other constants
const NAVIGATION_BUTTONS = ['recordBtn', 'prevBtn', 'skipBtn']; // Removed 'prevBtn', 'skipBtn'

// Add near the top after other constants
const MIN_FONT_SIZE = 17;
const MAX_FONT_SIZE = 33;
const FONT_SIZE_STEP = 2;

// Add to top with other state variables
let pendingUploads = new Map();

// Add constants at the top
const MAX_RECORDING_DURATION = 30000; // 30 seconds in milliseconds
let recordingTimeout;

// Add at the top of the file
let isAuthenticated = false;

// Add near the top with other variables
let audioPlayer = null;

// Add near the top with other state variables
let isSaving = false;

// Add this function at the top of the file
function getCSRFToken() {
    return document.querySelector('meta[name="csrf-token"]').getAttribute('content');
}

// Add these variables at the top of the file
let audioContext = null;
let scriptProcessor = null;
let audioInput = null;
let rawPCMData = [];
let isRecordingPCM = false;

// Add at the beginning of the file
function getCsrfToken() {
    const metaTag = document.querySelector('meta[name="csrf-token"]');
    if (!metaTag) {
        console.error("CSRF token meta tag not found");
        return '';
    }
    const token = metaTag.getAttribute('content');
    if (!token) {
        console.error("CSRF token is empty");
        return '';
    }
    return token;
}

// Function to populate country and state dropdowns
// Returns a Promise that resolves when the dropdowns are populated
function populateCountryDropdown() {
    return new Promise((resolve) => {
        const countrySelect = document.getElementById('country');
        const stateSelect = document.getElementById('state');

        if (!countrySelect || !stateSelect) {
            console.error('Country or state select element not found');
            resolve();
            return;
        }

        // Check if already populated
        if (countrySelect.options.length > 1) {
            // Already populated, just set up the change listener
            setupStateListener(countrySelect, stateSelect);
            resolve();
            return;
        }

        // Populate countries
        if (typeof country_and_states !== 'undefined' && country_and_states.country) {
            for (const [code, name] of Object.entries(country_and_states.country)) {
                const option = document.createElement('option');
                option.value = code;
                option.textContent = name;
                countrySelect.appendChild(option);
            }
        } else {
            console.error('country_and_states data not found');
        }

        // Set up the change event listener
        setupStateListener(countrySelect, stateSelect);

        resolve();
    });
}

function setupStateListener(countrySelect, stateSelect) {
    // Remove any existing change listeners to avoid duplicates
    const newCountrySelect = countrySelect.cloneNode(true);
    countrySelect.parentNode.replaceChild(newCountrySelect, countrySelect);

        // Update states based on selected country
        newCountrySelect.addEventListener('change', () => {
            const selectedCountry = newCountrySelect.value;
            const districtSelect = document.getElementById('district');
            
            // Clear state and district options
            stateSelect.innerHTML = '<option value="">Select State/Province</option>';
            if (districtSelect) districtSelect.innerHTML = '<option value="">Select District</option>';
            
            if (selectedCountry && country_and_states && country_and_states.states && country_and_states.states[selectedCountry]) {
                country_and_states.states[selectedCountry].forEach(state => {
                    const option = document.createElement('option');
                    option.value = state.code;
                    option.textContent = state.name;
                    stateSelect.appendChild(option);
                });
            }
        });

        // Trigger districts load if state is already selected (page load/pre-fill)
        if (stateSelect.value && typeof india_districts !== 'undefined' && india_districts[stateSelect.value]) {
            const districtSelect = document.getElementById('district');
            if (districtSelect) {
                districtSelect.innerHTML = '<option value="">Select District</option>';
                india_districts[stateSelect.value].forEach(district => {
                    const option = document.createElement('option');
                    option.value = district;
                    option.textContent = district;
                    districtSelect.appendChild(option);
                });
            }
        }

    // Add state change listener for districts (India only)
    stateSelect.addEventListener('change', () => {
        const selectedState = stateSelect.value;
        const districtSelect = document.getElementById('district');
        
        if (districtSelect) {
            districtSelect.innerHTML = '<option value="">Select District</option>';
            
            // Only populate for India states
            if (selectedState && typeof india_districts !== 'undefined' && india_districts[selectedState]) {
                india_districts[selectedState].forEach(district => {
                    const option = document.createElement('option');
                    option.value = district;
                    option.textContent = district;
                    districtSelect.appendChild(option);
                });
            }
        }
    });
}

// Load session data if exists
document.addEventListener('DOMContentLoaded', () => {
    // Check if authentication is disabled via meta tag
    const authEnabled = document.body.dataset.authEnabled === 'true';

    // If auth is disabled, consider the user authenticated
    isAuthenticated = !authEnabled || document.getElementById('authCheck')?.dataset.authenticated === 'true';

    if (!isAuthenticated && authEnabled) {
        // Show login required message only if auth is enabled
        document.getElementById('initialMessage').innerHTML = `
            <div class="text-center empty-state">
                <img src="/static/lock-icon.svg" alt="Lock" width="64" height="64">
                <h5 class="mt-4" style="color: #202124;">Authentication Required</h5>
                <p class="text-muted">
                    Please <a href="/login">sign in</a> to start recording.
                </p>
            </div>
        `;
        disableRecordingControls(true);
        return;
    }

    // Get user data for preferences
    const userDataElem = document.getElementById('userData');
    let userData = null;

    // Parse userData if it exists
    if (userDataElem) {
        try {
            userData = JSON.parse(userDataElem.textContent);
        } catch (e) {
            console.error('Error parsing user data:', e);
        }
    }

    // Load domains with user preferences only if domain element exists
    const domainSelect = document.getElementById('domain');
    if (domainSelect) {
        loadDomains(userData).then(() => {
            loadUserProfile();
        });
    } else {
        // Just load user profile if no domain element
        // Ensure country dropdown is populated first before loading profile
        populateCountryDropdown().then(() => {
            loadUserProfile();
        });
    }

    const savedSession = sessionStorage.getItem(SESSION_STORAGE_KEY);
    if (savedSession) {
        const sessionData = JSON.parse(savedSession);
        // Restore form values
        Object.keys(sessionData).forEach(key => {
            const element = document.getElementById(key);
            if (element) {
                element.value = sessionData[key];
                if (element.value) {
                    element.parentElement.classList.add('is-filled');
                }
            }
        });
    }

    // Initially disable recording interface
    const recordingInterface = document.getElementById('recordingInterface');
    recordingInterface.classList.add('disabled-interface');

    // Update interface state
    updateInterfaceState();


    // Keep this line to show 0/0 progress by default
    updateProgressDisplay(0, 0);

    // Restore saved font size
    const savedFontSize = localStorage.getItem('transcriptFontSize');
    if (savedFontSize) {
        const transcript = document.getElementById('currentTranscript');
        const sizeDisplay = document.getElementById('fontSizeDisplay');
        transcript.style.setProperty('--transcript-font-size', `${savedFontSize}px`);
        sizeDisplay.textContent = savedFontSize;
    }
});

// Modify loadDomains to accept user data and respect domain preferences
async function loadDomains(userData = null) {
    try {
        const domainSelect = document.getElementById('domain');
        domainSelect.innerHTML = '<option value="">Loading available domains...</option>';
        domainSelect.disabled = true;

        const response = await fetch('/domains');
        const data = await response.json();

        domainSelect.innerHTML = ''; // Clear any existing options

        if (data.status === 'success' && data.domains) {
            // Count available domains
            const domainCount = Object.keys(data.domains).length;

            if (domainCount === 0) {
                domainSelect.innerHTML = '<option value="" disabled>No domains available</option>';
                domainSelect.disabled = true;
                return;
            }

            // Get user's preferred domain (if available)
            const preferredDomain = userData?.domain || null;
            let preferredDomainExists = false;

            // Add domain options
            Object.entries(data.domains).forEach(([code, name]) => {
                const option = document.createElement('option');
                option.value = code;
                option.textContent = `${name} (${code})`;

                // Check if this is the user's preferred domain
                if (preferredDomain && code === preferredDomain) {
                    option.selected = true;
                    domainSelect.parentElement.classList.add('is-filled');
                    preferredDomainExists = true;
                }

                domainSelect.appendChild(option);
            });

            // If no preferred domain or it doesn't exist in options, select first option
            if (!preferredDomainExists && domainSelect.options.length > 0) {
                domainSelect.options[0].selected = true;
                domainSelect.parentElement.classList.add('is-filled');
            }

            // Enable the select
            domainSelect.disabled = false;

            // Load subdomains for selected domain
            if (domainSelect.value) {
                // Pass user preferences to loadSubdomains
                await loadSubdomains(domainSelect.value, userData?.subdomain || null);
            }
        } else {
            domainSelect.innerHTML = '<option value="" disabled>No domains available</option>';
            domainSelect.disabled = true;
        }
    } catch (error) {
        console.error('Error loading domains:', error);
        const domainSelect = document.getElementById('domain');
        domainSelect.innerHTML = '<option value="" disabled>Error loading domains</option>';
        domainSelect.disabled = true;
    }
}

// Modify loadSubdomains to accept preferred subdomain
async function loadSubdomains(domainCode, preferredSubdomain = null) {
    try {
        const subdomainSelect = document.getElementById('subdomain');

        if (!domainCode) {
            subdomainSelect.innerHTML = '<option value="">All Subdomains</option>';
            subdomainSelect.disabled = true;
            return;
        }

        subdomainSelect.innerHTML = '<option value="">Loading...</option>';
        subdomainSelect.disabled = true;

        const response = await fetch(`/domains/${domainCode}/subdomains`);
        const data = await response.json();

        subdomainSelect.innerHTML = ''; // Clear out loading message

        if (data.status === 'success' && data.subdomains) {
            let preferredSubdomainExists = false;

            data.subdomains.forEach(subdomain => {
                const option = document.createElement('option');
                option.value = subdomain.mnemonic;
                option.textContent = `${subdomain.name} (${subdomain.mnemonic})`;

                // Check if this is the user's preferred subdomain
                if (preferredSubdomain && subdomain.mnemonic === preferredSubdomain) {
                    option.selected = true;
                    subdomainSelect.parentElement.classList.add('is-filled');
                    preferredSubdomainExists = true;
                }

                subdomainSelect.appendChild(option);
            });

            // If no preferred subdomain or it doesn't exist in options, select first option
            if (!preferredSubdomainExists && subdomainSelect.options.length > 0) {
                subdomainSelect.options[0].selected = true;
                subdomainSelect.parentElement.classList.add('is-filled');
            }

            subdomainSelect.disabled = false;
        } else {
            // No subdomains available
            subdomainSelect.innerHTML = '<option value="">No subdomains available</option>';
            subdomainSelect.disabled = true;
        }
    } catch (error) {
        console.error('Error loading subdomains:', error);
        const subdomainSelect = document.getElementById('subdomain');
        subdomainSelect.innerHTML = '<option value="">Error loading subdomains</option>';
        subdomainSelect.disabled = true;
    }
}

// Update domain change handler to preserve preferences where possible
const domainElem = document.getElementById('domain');
if (domainElem) {
    domainElem.addEventListener('change', function () {
        // Get userData to potentially pass preferred subdomain
        const userDataElem = document.getElementById('userData');
        let preferredSubdomain = null;

        if (userDataElem) {
            try {
                const userData = JSON.parse(userDataElem.textContent);
                // Only use preferred subdomain if domain matches current selection
                if (userData.domain === this.value) {
                    preferredSubdomain = userData.subdomain;
                }
            } catch (e) {
                console.error('Error parsing user data:', e);
            }
        }

        // Load subdomains with preference
        loadSubdomains(this.value, preferredSubdomain);
    });
}

// Simplify loadUserProfile since domain/subdomain handling is now in loadDomains
function loadUserProfile() {
    const userDataElem = document.getElementById('userData');
    if (!userDataElem) return;

    try {
        const userData = JSON.parse(userDataElem.textContent);

        // Pre-fill basic form fields with user data (domains handled separately)
    const fields = ['gender', 'age_range', 'country', 'state', 'district', 'city', 'accent', 'language', 'education'];
        fields.forEach(field => {
            const elem = document.getElementById(field);
            if (elem && userData[field]) {
                elem.value = userData[field];
                elem.parentElement.classList.add('is-filled');

                // Handle country change to load states
                if (field === 'country' && userData['state']) {
                    const event = new Event('change');
                    elem.dispatchEvent(event);

                    setTimeout(() => {
                        const stateElem = document.getElementById('state');
                        if (stateElem) {
                            stateElem.value = userData['state'];
                            stateElem.parentElement.classList.add('is-filled');
                        }
                    }, 100);
                }
            }
        });

        // Note: Domain and subdomain are now handled by loadDomains and loadSubdomains
    } catch (e) {
        console.error('Error loading user profile:', e);
    }
}

// Add this function at the beginning
function updateButtonStates(state) {
    const states = {
        'initial': {
            'recordBtn': false,
            'playBtn': true,
            'saveBtn': true,
            'rerecordBtn': true,
            'prevBtn': false,
            'skipBtn': false
        },
        'recording': {
            'recordBtn': false, // Changed from true to false since it's a toggle
            'playBtn': true,
            'saveBtn': true,
            'rerecordBtn': true,
            'prevBtn': true,
            'skipBtn': true
        },
        'recorded': {
            'recordBtn': true,
            'playBtn': false,
            'saveBtn': false,
            'rerecordBtn': false,
            'prevBtn': true,
            'skipBtn': true
        },
        'saving': {
            'recordBtn': true,
            'playBtn': true,
            'saveBtn': true,
            'rerecordBtn': true,
            'prevBtn': false,
            'skipBtn': false
        }
    };

    const buttonStates = states[state];
    if (!buttonStates) return;

    Object.keys(buttonStates).forEach(buttonId => {
        const button = document.getElementById(buttonId);
        if (button) {
            button.disabled = buttonStates[buttonId];
        }
    });
}

// Replace the existing disableRecordingControls function with:
function disableRecordingControls(disabled = true) {
    if (disabled) {
        updateButtonStates('initial');
        // Disable navigation buttons
        NAVIGATION_BUTTONS.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.disabled = true;
            }
        });
    } else {
        // Enable navigation buttons
        NAVIGATION_BUTTONS.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.disabled = false;
            }
        });
    }
}

// Disable controls initially
document.addEventListener('DOMContentLoaded', () => {
    disableRecordingControls(true);
});

// Add near the top of the file after existing variable declarations
document.addEventListener('DOMContentLoaded', () => {
    const setupFormScroll = document.querySelector('.setup-form-scroll');

    setupFormScroll.addEventListener('scroll', () => {
        if (setupFormScroll.scrollTop > 0) {
            setupFormScroll.classList.add('scrolled');
        } else {
            setupFormScroll.classList.remove('scrolled');
        }
    });
});

// Add this function near the top
function showConfirmDialog(message) {
    return new Promise((resolve) => {
        const modal = document.createElement('div');
        modal.className = 'modal fade show';
        modal.style.display = 'block';
        modal.innerHTML = `
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content" style="border-radius: 8px; border: none; box-shadow: 0 2px 6px rgba(60, 64, 67, 0.15);">
                    <div class="modal-header" style="border-bottom: 1px solid #dadce0; padding: 16px 24px;">
                        <h5 class="modal-title" style="color: #202124; font-family: 'Google Sans', sans-serif; font-size: 16px;">Confirm Update</h5>
                        <button type="button" class="btn-close" style="color: #5f6368;" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body" style="padding: 24px; color: #5f6368;">
                        <p style="margin-bottom: 0; font-size: 14px;">${message}</p>
                    </div>
                    <div class="modal-footer" style="border-top: 1px solid #dadce0; padding: 16px 24px; gap: 8px;">
                        <button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-primary confirm-btn">Continue</button>
                    </div>
                </div>
            </div>
        `;

        const backdrop = document.createElement('div');
        backdrop.className = 'modal-backdrop fade show';
        backdrop.style.backgroundColor = 'rgba(32, 33, 36, 0.6)';

        document.body.appendChild(modal);
        document.body.appendChild(backdrop);
        document.body.classList.add('modal-open');

        const closeModal = () => {
            modal.remove();
            backdrop.remove();
            document.body.classList.remove('modal-open');
        };

        modal.querySelector('.btn-close').onclick = () => {
            closeModal();
            resolve(false);
        };

        modal.querySelector('.btn-outline-secondary').onclick = () => {
            closeModal();
            resolve(false);
        };

        modal.querySelector('.confirm-btn').onclick = () => {
            closeModal();
            resolve(true);
        };

        // Close on backdrop click
        backdrop.onclick = () => {
            closeModal();
            resolve(false);
        };
    });
}

// Modify the submit button handler
document.getElementById('sessionForm').addEventListener('submit', async function (e) {
    e.preventDefault();

    // Get CSRF token once
    const csrfToken = getCsrfToken();

    // Create form data
    const formData = new FormData();

    // Add CSRF token to form data
    formData.append('csrf_token', csrfToken);

    const speakerName = document.getElementById('speakerName')?.value || '';
    const gender = document.getElementById('gender')?.value || '';
    const language = document.getElementById('language')?.value || '';
    const country = document.getElementById('country')?.value || '';
    const state = document.getElementById('state')?.value || '';
    const city = document.getElementById('city')?.value || '';
    const ageRange = document.getElementById('age_range')?.value || '';
    const accent = document.getElementById('accent')?.value || '';
    const motherTongue = document.getElementById('mother_tongue')?.value || '';
    const customMotherTongue = document.getElementById('customMotherTongue')?.value || '';
    const education = document.getElementById('education')?.value || '';
    const district = document.getElementById('district')?.value || '';


    // Validate required fields
    if (!language) {
        showToast('Please select a language', 'error');
        return;
    }

    if (!motherTongue) {
        showToast('Please select a mother tongue', 'error');
        return;
    }

    if (!education) {
        showToast('Please select education level', 'error');
        return;
    }

    // Add form fields to formData with null checks
    formData.append('speakerName', speakerName);
    formData.append('gender', gender);
    formData.append('language', language);
    formData.append('country', country);
    formData.append('state', state);
    formData.append('district', district);
    formData.append('city', city);
    formData.append('age_range', ageRange);
    formData.append('accent', accent);
    formData.append('mother_tongue', motherTongue);
    formData.append('customMotherTongue', customMotherTongue);
    formData.append('education', education);

    const submitButton = document.querySelector('#sessionForm button[type="submit"]');
    const isUpdate = submitButton && submitButton.textContent === 'Update Session';

    if (isUpdate) {
        const confirmed = await showConfirmDialog('Warning: Updating the session will apply these changes to all future recordings. Continue?');
        if (!confirmed) {
            return;
        }
    }

    try {
        const response = await fetch('/start_session', {
            method: 'POST',
            headers: {
                'X-CSRF-Token': csrfToken
            },
            body: formData
        });

        let data;
        try {
            data = await response.json();
        } catch (e) {
            console.error("Error parsing JSON:", e);
        }

        // If we got a 403, it's likely a CSRF error
        if (response.status === 403) {
            showToast('Session authentication error. Please refresh the page and try again.', 'error');
            return;
        }

        // If we got a 401, it's an authentication error and we need to redirect to login
        if (response.status === 401) {
            if (data && data.code === 'AUTH_ERROR') {
                showToast('Your session has expired. Redirecting to login page...', 'error');
                setTimeout(() => {
                    window.location.href = '/login';
                }, 2000);
            } else if (data && data.error) {
                // Show the specific error if provided
                showToast(data.error, 'error');
            } else {
                showToast('Authentication error. Please login again.', 'error');
            }
            return;
        }
        if (response.ok) {
            // Update the speaker name field with the value from the server
            document.getElementById('speakerName').value = data.speaker_name;

            // Store session data in sessionStorage
            const sessionData = {
                gender: document.getElementById('gender').value,
                language: document.getElementById('language').value,
                country: document.getElementById('country').value,
                state: document.getElementById('state').value,
                city: document.getElementById('city').value,
                speaker_name: data.speaker_name,
                age_range: document.getElementById('age_range').value,
                accent: document.getElementById('accent').value,
                mother_tongue: document.getElementById('mother_tongue').value,
                education: document.getElementById('education').value
            };
            sessionStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(sessionData));

            // Show success message
            showToast('Session started successfully', 'success');

            // Update interface elements
            document.querySelector('.initial-message').style.display = 'none';
            document.querySelector('.transcript-container').style.display = 'block';

            // Enable recording interface
            const recordingInterface = document.getElementById('recordingInterface');
            recordingInterface.classList.remove('disabled-interface');
            disableRecordingControls(false);
            updateButtonStates('initial');

            // Load first transcript
            await loadNextTranscript();

            // Update submit button text
            submitButton.textContent = 'Update Session';

            // Update interface state
            updateInterfaceState();

            // Reload userData if it exists - this will refresh the session values on next load
            if (window.userData) {
                window.userData = {
                    ...window.userData,
                    gender,
                    language,
                    country,
                    state_province: state,
                    city,
                    age_range: ageRange,
                    accent
                };
            }

            // Close settings panel on mobile
            const settingsPanel = document.getElementById('settingsPanel');
            if (settingsPanel.classList.contains('show')) {
                settingsPanel.classList.remove('show');
                const overlay = document.querySelector('.overlay');
                if (overlay) {
                    overlay.classList.remove('show');
                    setTimeout(() => overlay.remove(), 300);
                }
            }

        } else {
            showToast(data.error || 'Failed to start session', 'error');
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('Error starting session', 'error');
    }
});

async function loadNextTranscript() {
    try {
        const response = await fetch('/next_transcript');
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Failed to load next transcript');
        }

        if (data.finished && data.current >= data.total) {
            showToast('Recording session completed!', 'success');
            setTimeout(() => {
                sessionStorage.removeItem(CURRENT_ROW_KEY);
                window.location.reload();
            }, 2000);
            return;
        }

        updateTranscriptDisplay(data);

        // Store current row if valid
        if (data.current > 0) {
            sessionStorage.setItem(CURRENT_ROW_KEY, data.current.toString());
        }

    } catch (error) {
        console.error('Error loading transcript:', error);
        showToast(error.message, 'error');
    }
}

// Add new function to update progress display
function updateProgressDisplay(current, total) {
    document.getElementById('progress').textContent = current;
    document.getElementById('total').textContent = total;
}

// Add these variables at the top
let audioStream = null;
let recorder = null;

// Fetch available microphones and populate the select element
async function populateMicrophones() {
    const microphoneSelect = document.getElementById('microphoneSelect');
    if (!microphoneSelect) return;
    
    try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        
        // Filter out default and communications virtual devices
        let audioInputDevices = devices.filter(device => 
            device.kind === 'audioinput' && 
            device.deviceId !== 'default' && 
            device.deviceId !== 'communications'
        );
        
        // If filtering removed everything, fallback to all audio inputs
        if (audioInputDevices.length === 0) {
            audioInputDevices = devices.filter(device => device.kind === 'audioinput');
        }
        
        // Deduplicate devices by groupId (represents physical hardware) or label
        const uniqueDevices = [];
        const seenGroups = new Set();
        const seenLabels = new Set();
        
        audioInputDevices.forEach(device => {
            const hasGroupId = device.groupId && device.groupId.trim() !== '';
            const hasLabel = device.label && device.label.trim() !== '';
            
            let isDuplicate = false;
            if (hasGroupId && seenGroups.has(device.groupId)) {
                isDuplicate = true;
            } else if (!hasGroupId && hasLabel && seenLabels.has(device.label)) {
                isDuplicate = true;
            }
            
            if (!isDuplicate) {
                if (hasGroupId) seenGroups.add(device.groupId);
                if (hasLabel) seenLabels.add(device.label);
                uniqueDevices.push(device);
            }
        });
        
        const currentValue = microphoneSelect.value;
        microphoneSelect.innerHTML = '';
        
        if (uniqueDevices.length === 0) {
            const option = document.createElement('option');
            option.value = '';
            option.textContent = 'Default Microphone';
            microphoneSelect.appendChild(option);
        } else {
            uniqueDevices.forEach((device, index) => {
                const option = document.createElement('option');
                option.value = device.deviceId;
                
                // Clean up label if it still has virtual prefixes
                let label = device.label || `Microphone ${index + 1}`;
                label = label.replace(/^Default(?: - )?/, '').replace(/^Communications(?: - )?/, '');
                
                option.textContent = label;
                microphoneSelect.appendChild(option);
            });
        }
        
        // Restore previous selection if still available
        if (currentValue && Array.from(microphoneSelect.options).some(opt => opt.value === currentValue)) {
            microphoneSelect.value = currentValue;
        }
    } catch (error) {
        console.error('Error fetching microphones:', error);
    }
}

// Keep the microphone list updated
if (navigator.mediaDevices) {
    navigator.mediaDevices.addEventListener('devicechange', populateMicrophones);
}
document.addEventListener('DOMContentLoaded', populateMicrophones);

// Replace getSupportedMimeType function with setupAudioContext
function setupAudioContext() {
    // Create audio context with 48kHz sample rate
    const AudioContext = window.AudioContext || window.webkitAudioContext;
    audioContext = new AudioContext({
        sampleRate: 48000,
        latencyHint: 'interactive'
    });
    return audioContext;
}

// Replace the record button handler with this version that includes the recording indicator
document.getElementById('recordBtn').addEventListener('click', async () => {
    const recordBtn = document.getElementById('recordBtn');
    const isRecording = recordBtn.classList.contains('recording');

    if (!isRecording) {
        // Start Recording
        try {
            // Set up audio context if it doesn't exist
            if (!audioContext) {
                audioContext = setupAudioContext();
            } else if (audioContext.state === 'suspended') {
                await audioContext.resume();
            }

            // Get user media stream
            const microphoneSelect = document.getElementById('microphoneSelect');
            const selectedDeviceId = microphoneSelect ? microphoneSelect.value : '';
            
            const audioConstraints = {
                channelCount: 1, // Mono
                sampleRate: 48000,
                echoCancellation: true,
                noiseSuppression: true,
                autoGainControl: true
            };
            
            if (selectedDeviceId) {
                audioConstraints.deviceId = { exact: selectedDeviceId };
            }

            audioStream = await navigator.mediaDevices.getUserMedia({
                audio: audioConstraints
            });
            
            // Update microphone list to get labels if permissions were just granted
            populateMicrophones();

            // Connect the stream to audio context
            audioInput = audioContext.createMediaStreamSource(audioStream);

            // Create script processor node for raw PCM capture
            scriptProcessor = audioContext.createScriptProcessor(4096, 1, 1);

            // Reset the raw PCM data array
            rawPCMData = [];
            isRecordingPCM = true;

            // Capture PCM data
            scriptProcessor.onaudioprocess = (event) => {
                if (isRecordingPCM) {
                    // Get the raw PCM data from the input channel
                    const inputData = event.inputBuffer.getChannelData(0);

                    // Create a copy of the float32 data
                    const pcmChunk = new Float32Array(inputData.length);
                    pcmChunk.set(inputData);

                    // Store the chunk
                    rawPCMData.push(pcmChunk);
                }
            };

            // Connect nodes
            audioInput.connect(scriptProcessor);
            scriptProcessor.connect(audioContext.destination);

            // Update UI with recording indicator
            recordBtn.innerHTML = '<span class="recording-dot"></span> Stop Recording';
            recordBtn.classList.add('recording', 'btn-danger', 'is-recording');
            recordBtn.classList.remove('btn-primary');
            updateButtonStates('recording');

            // Set timeout to stop recording after MAX_RECORDING_DURATION
            recordingTimeout = setTimeout(() => {
                if (isRecordingPCM) {
                    stopPCMRecording();
                    showToast('Maximum recording duration reached (30 seconds)', 'warning');
                }
            }, MAX_RECORDING_DURATION);

        } catch (err) {
            console.error('Audio recording error:', err);
            showToast('Error accessing microphone: ' + err.message, 'error');
        }
    } else {
        // Stop Recording
        stopPCMRecording();
    }
});

// Add a function to stop PCM recording
function stopPCMRecording() {
    if (isRecordingPCM) {
        // Stop recording
        isRecordingPCM = false;
        clearTimeout(recordingTimeout);

        // Disconnect nodes to free up resources
        if (scriptProcessor && audioInput) {
            audioInput.disconnect(scriptProcessor);
            scriptProcessor.disconnect(audioContext.destination);
        }

        // Stop tracks in the audio stream
        if (audioStream) {
            audioStream.getTracks().forEach(track => track.stop());
            audioStream = null;
        }

        // Create audio blob from raw PCM data
        processPCMData();

        // Update UI - restore original button state
        const recordBtn = document.getElementById('recordBtn');
        recordBtn.innerHTML = 'Start Recording'; // Remove dot and change text
        recordBtn.classList.remove('recording', 'btn-danger', 'is-recording');
        recordBtn.classList.add('btn-primary');
        updateButtonStates('recorded');
    }
}

// Modify function to process PCM data with proper fade-in, trim, and fade-out sequence
function processPCMData() {
    // First, determine the total length
    let totalLength = 0;
    for (const chunk of rawPCMData) {
        totalLength += chunk.length;
    }

    // Calculate parameters for fade effects and trimming
    const sampleRate = audioContext.sampleRate || 48000;
    const fadeInSamples = Math.min(sampleRate * 0.3, totalLength * 0.1); // 300ms fade in (max 10% of audio)
    const endTrimSamples = Math.min(sampleRate * 0.15, totalLength * 0.05); // 150ms trim at end (max 5% of audio)
    const fadeOutSamples = Math.min(sampleRate * 0.15, totalLength * 0.04); // 150ms fade out (max 4% of audio)

    // Step 1: Create a merged array with all chunks (before trimming)
    const fullMergedPCM = new Float32Array(totalLength);

    let offset = 0;
    for (const chunk of rawPCMData) {
        fullMergedPCM.set(chunk, offset);
        offset += chunk.length;
    }

    // Step 2: Apply fade-in effect to the beginning
    for (let i = 0; i < fadeInSamples; i++) {
        // Compute the fade-in multiplier (0 to 1)
        const fadeRatio = i / fadeInSamples;

        // Apply a smooth fade curve (cubic ease-in)
        const smoothFade = fadeRatio * fadeRatio * fadeRatio;

        // Apply the fade
        fullMergedPCM[i] *= smoothFade;
    }

    // Step 3: Create a trimmed version (excluding the end portion to be trimmed)
    const trimmedLength = Math.max(0, totalLength - endTrimSamples);
    const trimmedPCM = fullMergedPCM.slice(0, trimmedLength);

    // Step 4: Apply fade-out effect to the end of the trimmed audio
    const fadeOutStartIndex = trimmedLength - fadeOutSamples;
    for (let i = 0; i < fadeOutSamples; i++) {
        if (fadeOutStartIndex + i >= trimmedLength) break;

        // Compute the fade-out multiplier (1 to 0)
        const fadeRatio = 1 - (i / fadeOutSamples);

        // Apply a smooth fade curve (cubic ease-out)
        const smoothFade = fadeRatio * fadeRatio * fadeRatio;

        // Apply the fade
        trimmedPCM[fadeOutStartIndex + i] *= smoothFade;
    }

    // Convert float32 PCM to 16-bit PCM (Int16)
    const pcm16bit = convertFloat32ToInt16(trimmedPCM);

    // Create a blob with the processed PCM data
    audioBlob = new Blob([pcm16bit], { type: 'audio/pcm' });

    // Add a flag to metadata indicating the audio was processed
    audioProcessed = true;
}

// Add function to convert Float32 to Int16
function convertFloat32ToInt16(float32Array) {
    const int16Array = new Int16Array(float32Array.length);
    for (let i = 0; i < float32Array.length; i++) {
        // Convert floating point (-1 to 1) to 16-bit PCM
        // Clamp between -1 and 1, then scale to -32768 to 32767
        const s = Math.max(-1, Math.min(1, float32Array[i]));
        int16Array[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
    }
    return int16Array;
}

// Update the play button handler to work with raw PCM data properly
document.getElementById('playBtn').addEventListener('click', () => {
    if (audioBlob) {
        if (audioPlayer && !audioPlayer.paused) {
            // Stop playing if already playing
            audioPlayer.pause();
            document.getElementById('playBtn').textContent = 'Play';
            audioPlayer = null;
        } else {
            // Create AudioContext for playback
            const playbackContext = new (window.AudioContext || window.webkitAudioContext)();
            const reader = new FileReader();

            reader.onload = function (e) {
                try {
                    // Get the ArrayBuffer from the FileReader
                    const arrayBuffer = e.target.result;

                    // Convert the ArrayBuffer to Int16Array (assuming PCM data is 16-bit)
                    const int16Array = new Int16Array(arrayBuffer);

                    // Create an audio buffer with the same sample rate as recording
                    const sampleRate = audioContext ? audioContext.sampleRate : 48000;
                    const audioBuffer = playbackContext.createBuffer(1, int16Array.length, sampleRate);

                    // Get the audio buffer's first channel's Float32Array
                    const channelData = audioBuffer.getChannelData(0);

                    // Convert Int16 PCM back to Float32 format (-1.0 to 1.0)
                    for (let i = 0; i < int16Array.length; i++) {
                        // Convert Int16 [-32768, 32767] to Float32 [-1, 1]
                        channelData[i] = int16Array[i] / (int16Array[i] < 0 ? 32768 : 32767);
                    }

                    // Create audio source
                    const source = playbackContext.createBufferSource();
                    source.buffer = audioBuffer;
                    source.connect(playbackContext.destination);

                    // Play the audio
                    source.start(0);
                    document.getElementById('playBtn').textContent = 'Stop';

                    // Create a mock audio player interface
                    audioPlayer = {
                        pause: function () {
                            source.stop(0);
                        },
                        paused: false,
                        currentTime: 0
                    };

                    // When audio finishes playing
                    source.onended = function () {
                        document.getElementById('playBtn').textContent = 'Play';
                        audioPlayer = null;
                    };
                } catch (error) {
                    console.error('Error playing audio:', error);
                    showToast('Error playing audio: ' + error.message, 'error');
                    document.getElementById('playBtn').textContent = 'Play';
                    audioPlayer = null;
                }
            };

            // Read the blob as an array buffer
            reader.readAsArrayBuffer(audioBlob);
        }
    }
});

// Add function to stop playback
function stopPlayback() {
    if (audioPlayer) {
        audioPlayer.pause();
        document.getElementById('playBtn').textContent = 'Play';
        audioPlayer = null;
    }
}

// Update other button handlers to stop playback when clicked
['recordBtn', 'saveBtn', 'rerecordBtn'].forEach(buttonId => {
    document.getElementById(buttonId).addEventListener('click', stopPlayback);
});

// Modify the save button click handler
document.getElementById('saveBtn').addEventListener('click', async () => {
    // Disable all controls while saving
    isSaving = true;
    updateButtonStates('saving');

    // Create FormData with PCM audio and metadata
    const formData = new FormData();

    // Add CSRF token
    formData.append('csrf_token', getCsrfToken());

    // Add audio blob with PCM type and sampleRate metadata
    formData.append('audio', audioBlob, 'recording.pcm');
    formData.append('sampleRate', '48000');
    formData.append('bitsPerSample', '16');
    formData.append('channels', '1');
    formData.append('trimmed', 'true'); // Indicate audio was trimmed

    try {
        const response = await fetch('/save_recording', {
            method: 'POST',
            headers: {
                'X-CSRF-Token': getCsrfToken()
            },
            body: formData
        });

        if (response.ok) {
            const data = await response.json();

            if (data.storage.includes('huggingface')) {
                // Show initial upload toast
                showToast('Starting upload to Hugging Face...', 'info');

                // Track the new upload
                pendingUploads.set(data.upload_id, {
                    timestamp: Date.now(),
                    attempts: 0
                });

                // Start polling for this upload
                pollUploadStatus(data.upload_id);

                // Update UI to show upload progress
                updateUploadStatus();
            } else if (data.storage.includes('local')) {
                // Show local save toast
                showToast('Recording saved', 'success');
            } else if (data.storage.includes('memory')) {
                // Show memory save toast
                showToast('Recording saved in memory', 'success');
            }

            // Update transcript display with next transcript if available
            if (data.next_transcript) {
                document.getElementById('currentTranscript').textContent = data.next_transcript.text;
                updateProgressDisplay(data.next_transcript.current, data.next_transcript.total);
                sessionStorage.setItem(CURRENT_ROW_KEY, data.next_transcript.current.toString());
            } else if (data.session_complete) {
                showToast('Recording session completed!', 'success');
                setTimeout(() => window.location.reload(), 2000);
                return;
            }

            // Reset recording controls for next recording
            isSaving = false;
            updateButtonStates('initial');
            resetRecordingControls();

        } else {
            const error = await response.json();
            showToast('Error saving recording: ' + error.error, 'error');

            // Re-enable controls if save failed
            isSaving = false;
            updateButtonStates('recorded');
        }
    } catch (error) {
        showToast('Error saving recording: ' + error, 'error');

        // Re-enable controls if save failed
        isSaving = false;
        updateButtonStates('recorded');
    }
});

// Modify the rerecord button click handler
document.getElementById('rerecordBtn').addEventListener('click', () => {
    audioChunks = [];
    audioBlob = null;
    updateButtonStates('initial');
});

// Update the resetRecordingControls function
function resetRecordingControls() {
    if (mediaRecorder && mediaRecorder.stream) {
        mediaRecorder.stream.getTracks().forEach(track => track.stop());
    }
    clearTimeout(recordingTimeout);  // Clear timeout when resetting
    audioChunks = [];
    audioBlob = null;
    updateButtonStates('initial');

    // Reset recording button state
    const recordBtn = document.getElementById('recordBtn');
    recordBtn.innerHTML = 'Start Recording'; // Reset the inner HTML
    recordBtn.classList.remove('recording', 'btn-danger', 'is-recording');
    recordBtn.classList.add('btn-primary');
}

// Add this near the top of the file
document.querySelectorAll('.material-input .form-control').forEach(input => {
    // Add placeholder to maintain label position
    input.setAttribute('placeholder', ' ');

    // Handle autofill styling
    input.addEventListener('animationstart', function (e) {
        if (e.animationName === 'onAutoFillStart') {
            this.parentElement.classList.add('is-filled');
        }
    });

    input.addEventListener('input', function () {
        if (this.value) {
            this.parentElement.classList.add('is-filled');
        } else {
            this.parentElement.classList.remove('is-filled');
        }
    });
});

// Add at the start of the file
document.addEventListener('DOMContentLoaded', () => {
    // Settings panel toggle for mobile
    const settingsToggle = document.getElementById('settingsToggle');
    const settingsPanel = document.getElementById('settingsPanel');
    const body = document.body;

    if (settingsToggle) {
        settingsToggle.addEventListener('click', () => {
            settingsPanel.classList.toggle('show');

            // Create/toggle overlay
            let overlay = document.querySelector('.overlay');
            if (!overlay) {
                overlay = document.createElement('div');
                overlay.className = 'overlay';
                body.appendChild(overlay);
            }
            overlay.classList.toggle('show');

            // Close panel when clicking overlay
            overlay.addEventListener('click', () => {
                settingsPanel.classList.remove('show');
                overlay.classList.remove('show');
            });
        });
    }

    // Initially hide the transcript container
    const transcriptContainer = document.querySelector('.transcript-container');
    if (transcriptContainer) {
        transcriptContainer.style.display = 'none';
    }
});

// Add session clear on page unload
window.addEventListener('beforeunload', () => {
    // Optionally clear session storage when leaving page
    // sessionStorage.removeItem(SESSION_STORAGE_KEY);
});

// Update this function
function updateInterfaceState() {
    const recordingInterface = document.getElementById('recordingInterface');
    const initialMessage = document.getElementById('initialMessage');
    const transcriptContainer = document.querySelector('.transcript-container');

    if (!isAuthenticated) {
        recordingInterface.classList.add('disabled-interface');
        initialMessage.style.display = 'block';
        transcriptContainer.style.display = 'none';
        return;
    }

    const sessionData = sessionStorage.getItem(SESSION_STORAGE_KEY);

    if (!sessionData) {
        recordingInterface.classList.add('disabled-interface');
        initialMessage.style.display = 'block';
        transcriptContainer.style.display = 'none';

        // Show requirements without CSV reference
        initialMessage.innerHTML = `
            <div class="text-center empty-state">
                <img src="static/microphone-icon.svg" alt="Microphone" width="64" height="64">
                <h5 class="mt-4" style="color: #202124;">Interface Disabled</h5>
                <p class="text-muted">
                    Please complete all required settings to begin recording.
                </p>
                <div class="mt-3 requirements-list">
                    <div class="requirement ${sessionData ? 'complete' : 'incomplete'}">
                        <span class="icon">⬤</span>
                        <span>Complete Settings</span> 
                    </div>
                </div>
            </div>
        `;
    } else {
        recordingInterface.classList.remove('disabled-interface');
        initialMessage.style.display = 'none';
        transcriptContainer.style.display = 'block';
        disableRecordingControls(false);
    }
}

// Add these functions
function increaseFontSize() {
    const transcript = document.getElementById('currentTranscript');
    const sizeDisplay = document.getElementById('fontSizeDisplay');
    const currentSize = parseInt(window.getComputedStyle(transcript).fontSize);

    if (currentSize < MAX_FONT_SIZE) {
        const newSize = currentSize + FONT_SIZE_STEP;
        transcript.style.setProperty('--transcript-font-size', `${newSize}px`);
        sizeDisplay.textContent = newSize;
        localStorage.setItem('transcriptFontSize', newSize);
    }
}

function decreaseFontSize() {
    const transcript = document.getElementById('currentTranscript');
    const sizeDisplay = document.getElementById('fontSizeDisplay');
    const currentSize = parseInt(window.getComputedStyle(transcript).fontSize);

    if (currentSize > MIN_FONT_SIZE) {
        const newSize = currentSize - FONT_SIZE_STEP;
        transcript.style.setProperty('--transcript-font-size', `${newSize}px`);
        sizeDisplay.textContent = newSize;
        localStorage.setItem('transcriptFontSize', newSize);
    }
}

// Add keyboard shortcuts
document.addEventListener('keydown', (e) => {
    // Only handle shortcuts when interface is enabled
    if (document.getElementById('recordingInterface').classList.contains('disabled-interface')) {
        return;
    }

    // Don't trigger shortcuts when typing in input fields
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
        return;
    }

    const recordBtn = document.getElementById('recordBtn');

    switch (e.key.toLowerCase()) {
        case 'r':
            if (!recordBtn.disabled) {
                recordBtn.click();
            }
            break;
        case ' ': // Space key
            e.preventDefault(); // Prevent page scroll
            if (!document.getElementById('playBtn').disabled) {
                document.getElementById('playBtn').click();
            }
            break;
        case 'enter':
            if (!document.getElementById('saveBtn').disabled) {
                document.getElementById('saveBtn').click();
            }
            break;
        case 'backspace':
            if (!document.getElementById('rerecordBtn').disabled) {
                e.preventDefault(); // Prevent browser back
                document.getElementById('rerecordBtn').click();
            }
            break;
        case 'arrowleft':
            if (!document.getElementById('prevBtn').disabled) {
                document.getElementById('prevBtn').click();
            }
            break;
        case 'arrowright':
            if (!document.getElementById('skipBtn').disabled) {
                document.getElementById('skipBtn').click();
            }
            break;
    }
});

// Add scroll detection for transcript box
function updateTranscriptScrollState() {
    const transcriptBox = document.querySelector('.transcript-box');
    if (transcriptBox) {
        const isScrollable = transcriptBox.scrollHeight > transcriptBox.clientHeight;
        transcriptBox.classList.toggle('scrollable', isScrollable);
    }
}

// Update scroll state when transcript changes
const originalLoadNextTranscript = loadNextTranscript;
loadNextTranscript = async function () {
    await originalLoadNextTranscript.apply(this, arguments);
    updateTranscriptScrollState();
};

// Add scroll state check to DOMContentLoaded
document.addEventListener('DOMContentLoaded', () => {
    // ...existing DOMContentLoaded code...

    // Add scroll detection
    const transcriptBox = document.querySelector('.transcript-box');
    if (transcriptBox) {
        transcriptBox.addEventListener('scroll', () => {
            const hasReachedBottom =
                transcriptBox.scrollHeight - transcriptBox.scrollTop <= transcriptBox.clientHeight + 1;
            transcriptBox.classList.toggle('at-bottom', hasReachedBottom);
        });
    }
});

// Add new functions for upload management
function showToast(message, type = 'info') {
    const toast = document.getElementById('uploadToast');
    const toastHeader = toast.querySelector('.toast-header');
    const toastBody = toast.querySelector('.toast-body');

    // Remove existing classes and icon
    toast.classList.remove('info', 'success', 'error', 'warning');
    const oldIcon = toastHeader.querySelector('.toast-icon');
    if (oldIcon) oldIcon.remove();

    // Add appropriate class
    toast.classList.add(type);

    // Add icon based on type
    const icon = document.createElement('div');
    icon.className = 'toast-icon';
    icon.innerHTML = getToastIcon(type);
    toastHeader.insertBefore(icon, toastHeader.firstChild);

    toastBody.textContent = message;

    // Initialize and show toast
    const bsToast = new bootstrap.Toast(toast, {
        autohide: true,
        delay: 3000
    });
    bsToast.show();
}

function getToastIcon(type) {
    const icons = {
        info: `<svg viewBox="0 0 24 24" fill="#1a73e8">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z"/>
        </svg>`,
        success: `<svg viewBox="0 0 24 24" fill="#34a853">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
        </svg>`,
        error: `<svg viewBox="0 0 24 24" fill="#ea4335">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/>
        </svg>`,
        warning: `<svg viewBox="0 0 24 24" fill="#fbbc04">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 15h-2v-2h2v2zm0-4h-2V7h2v6z"/>
        </svg>`
    };
    return icons[type] || icons.info;
}

async function pollUploadStatus(uploadId) {
    const upload = pendingUploads.get(uploadId);
    if (!upload || upload.attempts > 30) {
        pendingUploads.delete(uploadId);
        updateUploadStatus();
        if (upload && upload.attempts > 30) {
            showToast('Upload timed out. Please try again.', 'warning');
        }
        return;
    }

    try {
        const response = await fetch(`/check_upload/${uploadId}`);
        const data = await response.json();

        if (data.complete) {
            pendingUploads.delete(uploadId);
            updateUploadStatus();
            showToast('Recording uploaded successfully! 🎉', 'success');
        } else {
            upload.attempts++;
            showToast(`Upload in progress... (${Math.round((upload.attempts / 30) * 100)}%)`, 'info');
            setTimeout(() => pollUploadStatus(uploadId), 2000);
        }
    } catch (error) {
        console.error('Error checking upload status:', error);
        upload.attempts++;
        setTimeout(() => pollUploadStatus(uploadId), 5000);
        showToast('Error checking upload status. Retrying...', 'error');
    }
}

function updateUploadStatus() {
    const statusContainer = document.getElementById('uploadStatus');
    if (!statusContainer) return;

    if (pendingUploads.size > 0) {
        showToast(`Uploading: ${pendingUploads.size} files pending...`);
        statusContainer.textContent = `Uploading: ${pendingUploads.size} pending`;
        statusContainer.style.display = 'block';
    } else {
        statusContainer.style.display = 'none';
    }
}

// Add near the top of the file after other document.ready handlers
document.addEventListener('DOMContentLoaded', () => {
    const consentCheckbox = document.getElementById('consentCheckbox');
    const startSessionBtn = document.getElementById('startSessionBtn');

    consentCheckbox.addEventListener('change', function () {
        startSessionBtn.disabled = !this.checked;
    });
});

// Add at the start of the file or with other DOM ready handlers
document.addEventListener('DOMContentLoaded', () => {
    // Settings panel toggle for mobile
    const settingsToggle = document.getElementById('settingsToggle');
    const settingsPanel = document.getElementById('settingsPanel');
    const settingsCloseBtn = document.getElementById('settingsCloseBtn');

    function closeSettingsPanel() {
        settingsPanel.classList.remove('show');
        const overlay = document.querySelector('.overlay');
        if (overlay) {
            overlay.classList.remove('show');
            setTimeout(() => overlay.remove(), 300); // Remove after transition
        }
    }

    if (settingsToggle) {
        settingsToggle.addEventListener('click', () => {
            settingsPanel.classList.add('show');

            // Create overlay if it doesn't exist
            if (!document.querySelector('.overlay')) {
                const overlay = document.createElement('div');
                overlay.className = 'overlay';
                document.body.appendChild(overlay);

                // Add click event to overlay
                overlay.addEventListener('click', closeSettingsPanel);

                // Show overlay after a brief delay to ensure smooth animation
                setTimeout(() => overlay.classList.add('show'), 10);
            }
        });
    }

    // Add click event for close button
    if (settingsCloseBtn) {
        settingsCloseBtn.addEventListener('click', closeSettingsPanel);
    }
});

// Add this function near the top
function clearSession() {
    // Clear session storage
    sessionStorage.removeItem(SESSION_STORAGE_KEY);
    sessionStorage.removeItem(CURRENT_ROW_KEY);

    // Reset current transcript and progress
    document.getElementById('currentTranscript').textContent = '';
    updateProgressDisplay(0, 0);

    // Reset interface state
    updateInterfaceState();

    // Reset recording controls
    resetRecordingControls();
    disableRecordingControls(true);
}

// Add this function for handling navigation errors
function handleNavigationError(data) {
    switch (data.code) {
        case 'NO_SESSION':
            showToast('Please start a session first', 'warning');
            break;
        case 'NO_ROW':
            showToast('Please enter a row number', 'warning');
            break;
        case 'INVALID_ROW':
            showToast(data.error || 'Invalid row number', 'warning');
            break;
        case 'DATA_ERROR':
            showToast('Error accessing transcript data', 'error');
            console.error('Data error:', data.details);
            break;
        default:
            showToast(data.error || 'Navigation error', 'error');
    }
}

// Update the updateTranscriptDisplay function to handle the badge
function updateTranscriptDisplay(data) {
    // Update transcript text
    document.getElementById('currentTranscript').textContent = data.transcript;

    // Update progress
    updateProgressDisplay(data.current, data.total);

    // Show/hide previously recorded badge
    const recordingStatus = document.getElementById('recordingStatus');
    if (recordingStatus) {
        if (data.previously_recorded) {
            recordingStatus.style.display = 'inline-block';
            recordingStatus.classList.remove('d-none');
        } else {
            recordingStatus.style.display = 'none';
            recordingStatus.classList.add('d-none');
        }
    }
}

// Update loadNextTranscript to use the new updateTranscriptDisplay function
async function loadNextTranscript() {
    try {
        const response = await fetch('/next_transcript');
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Failed to load next transcript');
        }

        if (data.finished && data.current >= data.total) {
            showToast('Recording session completed!', 'success');
            setTimeout(() => {
                sessionStorage.removeItem(CURRENT_ROW_KEY);
                window.location.reload();
            }, 2000);
            return;
        }

        updateTranscriptDisplay(data);

        // Store current row if valid
        if (data.current > 0) {
            sessionStorage.setItem(CURRENT_ROW_KEY, data.current.toString());
        }

    } catch (error) {
        console.error('Error loading transcript:', error);
        showToast(error.message, 'error');
    }
}

// Add debounce function to prevent multiple rapid clicks
function debounce(func, wait) {
    let timeout;
    return function (...args) {
        const context = this;
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(context, args), wait);
    };
}

// Add variables to track button states
let isPrevBtnDisabled = false;
let isSkipBtnDisabled = false;

// Replace the previous button click handler with improved debounced version
document.getElementById('prevBtn').addEventListener('click', debounce(async () => {
    // Prevent multiple clicks while processing
    if (isPrevBtnDisabled) return;

    try {
        // Disable the button immediately to prevent multiple clicks
        isPrevBtnDisabled = true;
        document.getElementById('prevBtn').disabled = true;

        const response = await fetch('/prev_transcript');
        const data = await response.json();

        if (response.ok) {
            // Even if we got a boundary error but still have transcript data,
            // update the display with what we have
            if (data.transcript) {
                updateTranscriptDisplay(data);
                sessionStorage.setItem(CURRENT_ROW_KEY, data.current.toString());
            }

            // Handle boundary condition
            if (data.code === 'BOUNDARY_ERROR') {
                showToast('Already at first transcript', 'info');
            }
        } else {
            handleNavigationError(data);
        }
    } catch (error) {
        console.error('Navigation error:', error);
        showToast('Error navigating transcripts', 'error');
    } finally {
        // Re-enable the button after a short delay
        setTimeout(() => {
            isPrevBtnDisabled = false;
            document.getElementById('prevBtn').disabled = false;
        }, 300);
    }
}, 300));

// Replace the skip button click handler with an improved version
document.getElementById('skipBtn').addEventListener('click', debounce(async () => {
    // Prevent multiple clicks while processing
    if (isSkipBtnDisabled) return;

    try {
        // Disable the button immediately to prevent multiple clicks
        isSkipBtnDisabled = true;
        document.getElementById('skipBtn').disabled = true;

        const response = await fetch('/skip_transcript');
        const data = await response.json();

        if (response.ok) {
            // Even if we got a boundary error but still have transcript data,
            // update the display with what we have
            if (data.transcript) {
                updateTranscriptDisplay(data);
                sessionStorage.setItem(CURRENT_ROW_KEY, data.current.toString());
            }

            // Handle boundary condition - always ensure Prev button is enabled
            // when we're at the end (even after multiple "Skip" clicks)
            if (data.code === 'BOUNDARY_ERROR') {
                showToast('Already at last transcript', 'info');
                // Make sure the previous button is ALWAYS enabled when at the end
                setTimeout(() => {
                    document.getElementById('prevBtn').disabled = false;
                    isPrevBtnDisabled = false;
                }, 100);
            }
        } else {
            handleNavigationError(data);
        }
    } catch (error) {
        console.error('Navigation error:', error);
        showToast('Error navigating transcripts', 'error');
    } finally {
        // Re-enable the button after a short delay
        setTimeout(() => {
            isSkipBtnDisabled = false;
            document.getElementById('skipBtn').disabled = false;

            // Additional safety check to ensure Prev button is enabled
            // if we received a boundary error
            if (document.getElementById('prevBtn').disabled) {
                document.getElementById('prevBtn').disabled = false;
                isPrevBtnDisabled = false;
            }
        }, 300);
    }
}, 300));
