// DOM Elements
const video = document.getElementById('video');
const player = document.getElementById('player');
const controlsContainer = document.getElementById('controlsContainer');
const progressBar = document.getElementById('progressBar');
const progress = document.getElementById('progress');
const bufferBar = document.getElementById('bufferBar');
const playBtn = document.getElementById('playBtn');
const muteBtn = document.getElementById('muteBtn');
const timeDisplay = document.getElementById('time');
const overlayPlay = document.getElementById('overlayPlay');
const rewindOverlay = document.getElementById('rewindOverlay');
const forwardOverlay = document.getElementById('forwardOverlay');
const titleBar = document.getElementById('titleBar');
const speedMenu = document.getElementById('speedMenu');
const speedButtons = speedMenu.querySelectorAll('button');
const rewindBtn = document.getElementById('rewindBtn');
const forwardBtn = document.getElementById('forwardBtn');
const volumeSlider = document.getElementById('volumeSlider');
const speedBtn = document.getElementById('speedBtn');
const pipBtn = document.getElementById('pipBtn');
const fullscreenBtn = document.getElementById('fullscreenBtn');
const hoverTime = document.getElementById('hoverTime');

// State variables
let hideControlsTimeout;
let isFullScreen = false;
let isDraggingProgress = false;
let mouseInControls = false;
let lastForwardTime = 0;
let lastRewindTime = 0;
let forwardCount = 0;
let rewindCount = 0;
let isVideoLoaded = false;
let lastActivity = Date.now();
let controlsVisible = true;
let mouseIsMoving = false;

// Initialize player
document.addEventListener('DOMContentLoaded', function() {
  updateTime();
  showControls();
  setupEventListeners();
  setupProgressBar();
  setupHoverEffects();
  
  // Initially style progress bar as disabled
  progressBar.classList.add('disabled');
});

function setupEventListeners() {
  // Button event listeners
  playBtn.addEventListener('click', togglePlay);
  rewindBtn.addEventListener('click', rewind);
  forwardBtn.addEventListener('click', forward);
  muteBtn.addEventListener('click', toggleMute);
  volumeSlider.addEventListener('input', () => setVolume(volumeSlider.value));
  speedBtn.addEventListener('click', toggleSpeedMenu);
  pipBtn.addEventListener('click', togglePiP);
  fullscreenBtn.addEventListener('click', toggleFullScreen);
  
  // Speed menu buttons
  speedButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      setSpeed(parseFloat(btn.dataset.speed));
    });
  });
  
  // Video event listeners
  video.addEventListener('play', () => {
    playBtn.innerHTML = '<i class="fas fa-pause"></i>';
    overlayPlay.classList.remove('show');
  });
  
  video.addEventListener('pause', () => {
    playBtn.innerHTML = '<i class="fas fa-play"></i>';
    overlayPlay.classList.add('show');
  });
  
  video.addEventListener('timeupdate', updateProgress);
  video.addEventListener('click', togglePlay);
  
  // Improve mouse movement tracking
  player.addEventListener('mousemove', handleMouseMove);
  player.addEventListener('mouseleave', () => {
    // Only hide controls when in fullscreen and video is playing
    if (isFullScreen && !video.paused) {
      hideControlsTimeout = setTimeout(hideControls, 3000);
    }
  });
  
  // Buffer progress tracking
  video.addEventListener('progress', updateBufferProgress);
  
  // Video loading events
  video.addEventListener('loadedmetadata', () => {
    isVideoLoaded = true;
    progressBar.classList.remove('disabled');
    updateTime();
    if (titleBar) {
      titleBar.style.display = 'block';
    }
  });
  
  video.addEventListener('canplay', () => {
    isVideoLoaded = true;
    progressBar.classList.remove('disabled');
  });
  
  // Error event in case video fails to load
  video.addEventListener('error', () => {
    isVideoLoaded = false;
    progressBar.classList.add('disabled');
  });
  
  // Keyboard controls
  document.addEventListener('keydown', (e) => {
    if (e.code === 'Space') {
      e.preventDefault();
      togglePlay();
    } else if (e.code === 'ArrowLeft') {
      rewind();
    } else if (e.code === 'ArrowRight') {
      forward();
    } else if (e.code === 'ArrowUp') {
      e.preventDefault();
      setVolume(Math.min(1, video.volume + 0.1));
      volumeSlider.value = video.volume;
    } else if (e.code === 'ArrowDown') {
      e.preventDefault();
      setVolume(Math.max(0, video.volume - 0.1));
      volumeSlider.value = video.volume;
    } else if (e.code === 'KeyM') {
      toggleMute();
    } else if (e.code === 'KeyF') {
      toggleFullScreen();
    }
  });
  
  // Fullscreen listeners
  document.addEventListener('fullscreenchange', handleFullscreenChange);
  document.addEventListener('webkitfullscreenchange', handleFullscreenChange);
  document.addEventListener('mozfullscreenchange', handleFullscreenChange);
  document.addEventListener('MSFullscreenChange', handleFullscreenChange);
  
  // Controls area tracking
  controlsContainer.addEventListener('mouseenter', () => {
    mouseInControls = true;
    showControls();
    clearTimeout(hideControlsTimeout);
  });
  
  controlsContainer.addEventListener('mouseleave', () => {
    mouseInControls = false;
    // Only set timeout to hide controls if in fullscreen mode and video is playing
    if (isFullScreen && !video.paused) {
      hideControlsTimeout = setTimeout(hideControls, 3000);
    }
  });
  
  // Hide speed menu when clicking outside
  document.addEventListener('click', (e) => {
    if (!speedBtn.contains(e.target) && !speedMenu.contains(e.target)) {
      speedMenu.style.display = 'none';
    }
  });
}

function updateBufferProgress() {
  if (video.buffered.length > 0) {
    const bufferedEnd = video.buffered.end(video.buffered.length - 1);
    const duration = video.duration;
    if (duration > 0) {
      const bufferedPercent = (bufferedEnd / duration) * 100;
      bufferBar.style.width = `${bufferedPercent}%`;
    }
  }
}

function handleMouseMove() {
  lastActivity = Date.now();
  showControls();
  
  if (!mouseIsMoving) {
    mouseIsMoving = true;
    setTimeout(() => { mouseIsMoving = false; }, 100);
  }
  
  // Only set timeout to hide controls if in fullscreen mode and video is playing
  if (isFullScreen && !video.paused) {
    clearTimeout(hideControlsTimeout);
    hideControlsTimeout = setTimeout(checkActivity, 3000);
  }
}

function checkActivity() {
  const idleTime = Date.now() - lastActivity;
  if (idleTime > 2800 && !mouseInControls && !isDraggingProgress && !video.paused && isFullScreen) {
    hideControls();
  } else if (isFullScreen && !video.paused) {
    hideControlsTimeout = setTimeout(checkActivity, 1000);
  }
}

function handleFullscreenChange() {
  isFullScreen = !!document.fullscreenElement || 
                 !!document.webkitFullscreenElement || 
                 !!document.mozFullScreenElement ||
                 !!document.msFullscreenElement;
  
  if (isFullScreen) {
    fullscreenBtn.innerHTML = '<i class="fas fa-compress"></i>';
    // Only start hiding controls countdown if video is playing
    if (!video.paused) {
      hideControlsTimeout = setTimeout(checkActivity, 3000);
    }
  } else {
    fullscreenBtn.innerHTML = '<i class="fas fa-expand"></i>';
    showControls(); // Make sure controls are always visible when exiting fullscreen
    clearTimeout(hideControlsTimeout); 
  }
}

// Show controls
function showControls() {
  if (!controlsVisible) {
    controlsContainer.classList.remove('hidden');
    if (titleBar) {
      titleBar.style.opacity = '1';
    }
    controlsVisible = true;
  }
  clearTimeout(hideControlsTimeout);
}

// Hide controls (fullscreen only)
function hideControls() {
  // Only hide controls if in fullscreen mode, video is playing, and no interaction is happening
  if (isFullScreen && !video.paused && !mouseInControls && !isDraggingProgress && !mouseIsMoving) {
    controlsContainer.classList.add('hidden');
    if (titleBar) {
      titleBar.style.opacity = '0';
    }
    controlsVisible = false;
  }
}

// Play/Pause function
function togglePlay() {
  if (video.paused) {
    video.play().catch(err => {
      console.error("Play failed:", err);
    });
  } else {
    video.pause();
  }
  showControls();
  
  // Reset hiding controls timer if in fullscreen and playing
  if (isFullScreen && !video.paused) {
    clearTimeout(hideControlsTimeout);
    hideControlsTimeout = setTimeout(checkActivity, 3000);
  }
}

// Rewind function with 10s increments
function rewind() {
  const now = Date.now();
  const jumpAmount = 10 * (rewindCount + 1);
  
  if (now - lastRewindTime > 1000) {
    rewindCount = 0;
  }
  
  video.currentTime = Math.max(0, video.currentTime - jumpAmount);
  rewindOverlay.innerHTML = `<i class="fas fa-backward"></i> ${jumpAmount}s`;
  rewindOverlay.classList.add('show');
  
  setTimeout(() => {
    rewindOverlay.classList.remove('show');
  }, 700);
  
  lastRewindTime = now;
  rewindCount++;
  setTimeout(() => {
    if (Date.now() - lastRewindTime > 1000) {
      rewindCount = 0;
    }
  }, 1000);
  
  showControls();
}

// Forward function with 10s increments
function forward() {
  const now = Date.now();
  const jumpAmount = 10 * (forwardCount + 1);
  
  if (now - lastForwardTime > 1000) {
    forwardCount = 0;
  }
  
  video.currentTime = Math.min(video.duration, video.currentTime + jumpAmount);
  forwardOverlay.innerHTML = `${jumpAmount}s <i class="fas fa-forward"></i>`;
  forwardOverlay.classList.add('show');
  
  setTimeout(() => {
    forwardOverlay.classList.remove('show');
  }, 700);
  
  lastForwardTime = now;
  forwardCount++;
  setTimeout(() => {
    if (Date.now() - lastForwardTime > 1000) {
      forwardCount = 0;
    }
  }, 1000);
  
  showControls();
}

// Mute toggle function
function toggleMute() {
  video.muted = !video.muted;
  muteBtn.innerHTML = video.muted ? '<i class="fas fa-volume-mute"></i>' : '<i class="fas fa-volume-up"></i>';
  showControls();
}

// Set volume
function setVolume(val) {
  video.volume = val;
  volumeSlider.value = val;
  video.muted = val === 0;
  
  // Update volume icon based on level
  if (val === 0 || video.muted) {
    muteBtn.innerHTML = '<i class="fas fa-volume-mute"></i>';
  } else if (val < 0.5) {
    muteBtn.innerHTML = '<i class="fas fa-volume-down"></i>';
  } else {
    muteBtn.innerHTML = '<i class="fas fa-volume-up"></i>';
  }
  
  showControls();
}

// Update progress bar
function updateProgress() {
  if (!isDraggingProgress && video.duration) {
    const percent = (video.currentTime / video.duration) * 100;
    progress.style.width = percent + '%';
  }
  updateTime();
}

// Seek function with dragging
function setupProgressBar() {
  progressBar.addEventListener('mousedown', (e) => {
    // Only allow dragging if video is loaded
    if (isVideoLoaded) {
      isDraggingProgress = true;
      seek(e);
      showControls();
    }
  });

  document.addEventListener('mousemove', (e) => {
    if (isDraggingProgress && isVideoLoaded) {
      seek(e);
      showControls();
    }
  });

  document.addEventListener('mouseup', () => {
    if (isDraggingProgress) {
      isDraggingProgress = false;
      showControls();
      
      // Reset hiding controls timer after dragging if in fullscreen and playing
      if (isFullScreen && !video.paused) {
        clearTimeout(hideControlsTimeout);
        hideControlsTimeout = setTimeout(checkActivity, 3000);
      }
    }
  });

  progressBar.addEventListener('click', (e) => {
    // Only allow seeking if video is loaded
    if (isVideoLoaded) {
      seek(e);
    }
  });
}

function setupHoverEffects() {
  progressBar.addEventListener('mousemove', (e) => {
    if (isVideoLoaded && video.duration) {
      const rect = progressBar.getBoundingClientRect();
      const percent = (e.clientX - rect.left) / rect.width;
      const time = percent * video.duration;
      
      hoverTime.textContent = formatTime(time);
      hoverTime.style.left = e.clientX - rect.left + 'px';
    }
  });
  
  progressBar.addEventListener('mouseenter', () => {
    if (isVideoLoaded) {
      hoverTime.style.opacity = '1';
    }
  });
  
  progressBar.addEventListener('mouseleave', () => {
    hoverTime.style.opacity = '0';
  });
}

function seek(e) {
  if (!isVideoLoaded) return; // Safety check
  
  const rect = progressBar.getBoundingClientRect();
  let percent = (e.clientX - rect.left) / rect.width;
  percent = Math.max(0, Math.min(1, percent));
  progress.style.width = (percent * 100) + '%';
  
  if (video.duration) {
    video.currentTime = percent * video.duration;
  }
}

// Format time as HH:MM:SS
function formatTime(seconds) {
  if (isNaN(seconds)) return '00:00:00';
  
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  
  return [
    h.toString().padStart(2, '0'),
    m.toString().padStart(2, '0'),
    s.toString().padStart(2, '0')
  ].join(':');
}

// Update time display
function updateTime() {
  const current = video.currentTime || 0;
  const duration = video.duration || 0;
  timeDisplay.innerText = `${formatTime(current)} / ${formatTime(duration)}`;
}

// Speed menu toggle
function toggleSpeedMenu() {
  speedMenu.style.display = speedMenu.style.display === 'flex' ? 'none' : 'flex';
  showControls();
}

// Set speed
function setSpeed(speed) {
  video.playbackRate = speed;
  speedMenu.style.display = 'none';
  
  // Update active button in speed menu
  speedButtons.forEach(btn => {
    btn.classList.remove('active');
    if (parseFloat(btn.dataset.speed) === speed) {
      btn.classList.add('active');
    }
  });
  
  showControls();
}

// Fullscreen toggle
function toggleFullScreen() {
  if (isFullScreen) {
    if (document.exitFullscreen) document.exitFullscreen();
    else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
    else if (document.mozCancelFullScreen) document.mozCancelFullScreen();
    else if (document.msExitFullscreen) document.msExitFullscreen();
  } else {
    if (player.requestFullscreen) player.requestFullscreen();
    else if (player.webkitRequestFullscreen) player.webkitRequestFullscreen();
    else if (player.mozRequestFullScreen) player.mozRequestFullScreen();
    else if (player.msRequestFullscreen) player.msRequestFullscreen();
  }
}
// Add touch controls for mobile
const playerArea = document.createElement('div');
playerArea.className = 'player-touch-areas';
player.appendChild(playerArea);

// Create touch areas (left, center, right)
const leftArea = document.createElement('div');
leftArea.className = 'touch-area left-area';
leftArea.addEventListener('click', rewind);

const centerArea = document.createElement('div');
centerArea.className = 'touch-area center-area';
centerArea.addEventListener('click', togglePlay);

const rightArea = document.createElement('div');
rightArea.className = 'touch-area right-area';
rightArea.addEventListener('click', forward);

playerArea.appendChild(leftArea);
playerArea.appendChild(centerArea);
playerArea.appendChild(rightArea);

// Handle orientation change for mobile fullscreen
window.addEventListener('orientationchange', handleOrientationChange);
// Handle orientation changes for mobile devices
function handleOrientationChange() {
  if (isFullScreen) {
    // Force landscape orientation when in fullscreen on mobile
    if (window.orientation === 0 || window.orientation === 180) {
      // Phone is in portrait mode while fullscreen, try to force landscape
      if ('orientation' in screen && 'lock' in screen.orientation) {
        screen.orientation.lock('landscape').catch(e => {
          // Handle cases where orientation locking isn't supported
          console.log('Orientation lock not supported');
        });
      }
    }
  } else if (document.fullscreenElement === null) {
    // When exiting fullscreen, unlock the orientation
    if ('orientation' in screen && 'unlock' in screen.orientation) {
      screen.orientation.unlock();
    }
  }
}
// Fullscreen toggle with mobile support
function toggleFullScreen() {
  if (isFullScreen) {
    if (document.exitFullscreen) document.exitFullscreen();
    else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
    else if (document.mozCancelFullScreen) document.mozCancelFullScreen();
    else if (document.msExitFullscreen) document.msExitFullscreen();
  } else {
    if (player.requestFullscreen) player.requestFullscreen();
    else if (player.webkitRequestFullscreen) player.webkitRequestFullscreen();
    else if (player.mozRequestFullScreen) player.mozRequestFullScreen();
    else if (player.msRequestFullscreen) player.msRequestFullscreen();
    
    // Try to force landscape on mobile
    if (window.innerWidth < 768 && window.matchMedia("(orientation: portrait)").matches) {
      if ('orientation' in screen && 'lock' in screen.orientation) {
        screen.orientation.lock('landscape').catch(e => {
          // Orientation lock not supported or denied by user
          console.log('Could not lock orientation: ', e);
        });
      }
    }
  }
}
// Picture in Picture toggle
function togglePiP() {
  if (video !== document.pictureInPictureElement) {
    video.requestPictureInPicture().catch(err => {
      console.error("PiP failed:", err);
    });
  } else {
    document.exitPictureInPicture().catch(err => {
      console.error("Exit PiP failed:", err);
    });
  }
  showControls();
}