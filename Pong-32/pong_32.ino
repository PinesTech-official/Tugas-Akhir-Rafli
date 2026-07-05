#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WiFiManager.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <HTTPClient.h>
#include <Preferences.h>


// ─── Server ───────────────────────────────────────────────────────────────────
#define SERVER_HOST       "https://siabel.poltera.ac.id"
#define SERVER_PATH       "/iot"

// ─── MQTT ────────────────────────────────────────────────────────────────────
#define MQTT_BROKER    "mqtt.icminovasi.my.id"
#define MQTT_PORT      8883
#define MQTT_USERNAME  "smartfarming"
#define MQTT_PASSWORD  "coc@poltera.ac.id"
#define MQTT_CLIENT_ID "pong-45c4"
#define MQTT_TOPIC     "smartfarming/pong-45c4/tiktok"

// Nama AP yang muncul saat konfigurasi WiFi pertama kali
#define WIFIMANAGER_AP_NAME "Pong-Setup"

// ─── Tombol ──────────────────────────────────────────────────────────────────
#define BTN_PIN           5       // GPIO5, active low
#define DEBOUNCE_MS       50
#define LONG_PRESS_MS     3000    // tahan 3 detik → buka ulang portal konfigurasi

// ─── MQTT reconnect interval ─────────────────────────────────────────────────
#define MQTT_RETRY_MS     5000

// ─── Aktuator ────────────────────────────────────────────────────────────────
// RELAY: bukan untuk motor pelontar — motor pelontar pakai L298N langsung
#define RELAY_PIN         4
// Motor Peluncur (L298N) — dikontrol langsung via IN1/IN2 + PWM_PIN_A
#define IN1_PIN_A         27
#define IN2_PIN_A         26
// Motor Peluncur bawah (L298N) — dikontrol langsung via IN3/IN4 + PWM_PIN_B
#define IN3_PIN_B         33
#define IN4_PIN_B         32
#define PWM_PIN_A         25      // ENA — Motor Peluncur Atas
#define PWM_PIN_B         14      // ENB — Motor Peluncur Bawah
#define PWM_FREQ          5000    // Hz
#define PWM_RESOLUTION    12      // bit (0–4095)
#define PWM_MAX           4095

// ─── Kecepatan Motor (skala 0–255, dimap ke 12-bit 0–4095) ───────────────────
// map8to12(x) = x * 4095 / 255
#define SPEED_NORMAL_RAW  180     // kecepatan normal (skala 0–255)
#define SPEED_MAX_RAW     255     // kecepatan maksimum (skala 0–255)
#define SPEED_NORMAL      ((uint32_t)(180UL * 4095 / 255))  // ≈ 2890 (12-bit)
#define SPEED_MAX         PWM_MAX                           // 4095  (12-bit)
// Bounce: motor atas pelan (130), motor bawah full → bola terlontar melambung ke atas
#define SPEED_BOUNCE_A    ((uint32_t)(130UL * 4095 / 255))  // motor atas: pelan (130 → ≈2090 12-bit)
#define SPEED_BOUNCE_B    SPEED_MAX                         // motor bawah: full speed (4095)

// ─── Timing Motor ────────────────────────────────────────────────────────────
#define RELAY_LIKE_MS        2500   // relay aktif 2500 ms per like
#define RELAY_GIFT_MS        7500   // relay aktif 7500 ms per gift slot
#define STORAGE_PER_BALL_MS  800   // 1 bola = 800ms relay aktif
#define LAUNCHER_SPOOLUP_MS  800

// ─── Deduplication like ───────────────────────────────────────────────────────
// FIX #1: tambah dedup untuk like agar data tidak di-eksekusi 2x
#define LIKE_DEDUP_MS     800     // jendela dedup like (ms)
uint32_t lastLikeHash     = 0;
uint32_t lastLikeHashTime = 0;

// ─── EVENT TYPES ─────────────────────────────────────────────────────────────
#define EVENT_IDLE        0
#define EVENT_LIKE        1
#define EVENT_CHAT        2
#define EVENT_GIFT        3
#define EVENT_FOLLOW      4
#define EVENT_SHARE       5

// ─── FIX #2 & #5: Stack langsung tanpa sistem antrian terpisah, 500 slot ─────
// Antrian dihapus — array stack langsung di-push dan di-pop oleh processStack
#define STACK_SIZE        500     // FIX #5: slot diubah jadi 500

struct StackEntry {
  bool    filled;
  uint8_t eventType;
  int     diamondCount;
  int     repeatCount;
  int     likeCount;
};

StackEntry eventStack[STACK_SIZE];  // semua filled=false saat boot
int        writeIdx = 0;
int        readIdx  = 0;

// ─── State mesin motor non-blocking ──────────────────────────────────────────
enum MotorPhase {
  PHASE_IDLE = 0,
  PHASE_LAUNCHER_SPOOLUP,
  PHASE_BOUNCE_DELAY,    // delay motor B untuk efek bounce (gift >= 5 coins)
  PHASE_STORAGE_RUN,
  PHASE_DONE
};

MotorPhase motorPhase    = PHASE_IDLE;
uint32_t   phaseStartMs  = 0;
int        pendingBalls  = 0;
uint32_t   pendingSpeed  = 0;   // kecepatan motor A (12-bit)
uint32_t   pendingSpeedB = 0;   // kecepatan motor B (12-bit) — bisa beda saat bounce
int        pendingBounce = 0;

// FIX #3: relay tetap ON selama ada bola yang ditembak, tidak mati antar slot
// Relay dikelola manual: ON saat mulai tembak, OFF hanya saat semua bola selesai
bool     relayActive = false;

// ─── Deduplication gift ───────────────────────────────────────────────────────
uint32_t lastGiftHash     = 0;
uint32_t lastGiftHashTime = 0;
#define  GIFT_DEDUP_MS      1500
// Fallback timer: jika repeatEnd=true tidak datang dalam X ms setelah repeatEnd=false,
// push gift tersebut ke stack (handle gift non-combo / non-streakable)
#define  GIFT_FALLBACK_MS   600   // 600ms cukup untuk menunggu repeatEnd=true
// Post-push cooldown: setelah gift di-push, tolak gift dengan hash SAMA selama X ms
// Menangani server TikTok yang kirim 2 event identik dengan jeda hingga beberapa detik
#define  GIFT_COOLDOWN_MS   5000  // 5 detik cooldown setelah push
uint32_t lastPushedGiftHash   = 0;
uint32_t lastPushedGiftTimeMs = 0;

struct PendingGift {
  bool     valid;
  uint32_t hash;
  uint32_t receivedAt;
  int      diamondCount;
  int      repeatCount;
};
PendingGift pendingGift = { false, 0, 0, 0, 0 };

// ─── Mode bounce ─────────────────────────────────────────────────────────────
#define BOUNCE_MODE_NONE   0
#define BOUNCE_MODE_ACTIVE 1
// Bounce: motor B (spin feeder) di-delay sekian ms agar bola memantul
#define BOUNCE_DELAY_MS    300    // delay motor B saat bounce mode

// ─────────────────────────────────────────────────────────────────────────────

WiFiClientSecure wifiClient;
PubSubClient     mqttClient(wifiClient);
Preferences      prefs;

char     tiktokUsername[64] = "";
bool     tiktokConnected    = false;
bool     lastBtnState       = HIGH;
uint32_t lastDebounceTime   = 0;
uint32_t btnPressStart      = 0;
uint32_t lastMqttAttempt    = 0;
bool     motorReady         = false;

// ─────────────────────────────────────────────────────────────────────────────
// MOTOR HELPERS
// ─────────────────────────────────────────────────────────────────────────────
void initMotors() {
  ledcAttach(PWM_PIN_A, PWM_FREQ, PWM_RESOLUTION);
  ledcAttach(PWM_PIN_B, PWM_FREQ, PWM_RESOLUTION);
  ledcWrite(PWM_PIN_A, 0);
  ledcWrite(PWM_PIN_B, 0);
  motorReady = true;
  Serial.println("[Motor] PWM siap.");
}

void stopSemuaMotor() {
  if (!motorReady) return;
  // Motor peluncur atas (L298N)
  digitalWrite(IN1_PIN_A, LOW);
  digitalWrite(IN2_PIN_A, LOW);
  ledcWrite(PWM_PIN_A, 0);
  // Motor peluncur bawah (L298N)
  digitalWrite(IN3_PIN_B, LOW);
  digitalWrite(IN4_PIN_B, LOW);
  ledcWrite(PWM_PIN_B, 0);
  // Motor feeder (relay) OFF
  digitalWrite(RELAY_PIN, HIGH);  // active LOW → OFF
  relayActive = false;
  Serial.println("[RELAY] OFF → feeder berhenti");
}

// ─────────────────────────────────────────────────────────────────────────────
// PUSH EVENT ke stack
// ─────────────────────────────────────────────────────────────────────────────
void pushEvent(uint8_t type, int diamond, int repeat, int likes, const char* caller="?") {
  // Hitung slot terisi
  int used = (writeIdx - readIdx + STACK_SIZE) % STACK_SIZE;
  if (used >= STACK_SIZE - 1) {
    Serial.println("[STACK] Penuh! Event dilewati.");
    return;
  }
  eventStack[writeIdx] = { true, type, diamond, repeat, likes };
  writeIdx = (writeIdx + 1) % STACK_SIZE;
  Serial.printf("[STACK] Push dari [%s] → type=%d diamond=%d repeat=%d writeIdx=%d readIdx=%d\n",
                caller, type, diamond, repeat, writeIdx, readIdx);
}

// ─────────────────────────────────────────────────────────────────────────────
// PROCESS MOTOR (non-blocking state machine)
// FIX #3: relay tidak mati saat perpindahan slot — relay ON terus selama
//         motor aktif, OFF hanya ketika stack kosong dan motor selesai.
// FIX #4: motor pelontar (A & B) SEPENUHNYA dikontrol L298N (IN1/IN2/IN3/IN4 + PWM).
//         Relay HANYA untuk motor feeder — tidak pernah mengontrol motor A atau B.
// ─────────────────────────────────────────────────────────────────────────────
void processMotor() {
  if (!motorReady) return;
  uint32_t now = millis();

  switch (motorPhase) {

    // ── IDLE: ada event di stack? Langsung eksekusi ────────────────────────
    case PHASE_IDLE: {
      if (!eventStack[readIdx].filled) return;

      StackEntry& ev = eventStack[readIdx];
      eventStack[readIdx].filled = false;
      readIdx = (readIdx + 1) % STACK_SIZE;

      // Tentukan parameter tembak dari eventType
      if (ev.eventType == EVENT_LIKE) {
        int likeCount = ev.likeCount < 1 ? 1 : ev.likeCount;
        pendingBalls  = likeCount;       // 1 like = 1 bola
        pendingSpeed  = SPEED_NORMAL;    // kecepatan normal (180 → 12-bit)
        pendingSpeedB = SPEED_NORMAL;
        pendingBounce = BOUNCE_MODE_NONE;
        Serial.printf(">>> LIKE +%d → %d bola (speed normal)\n", likeCount, pendingBalls);

      } else if (ev.eventType == EVENT_GIFT) {
        int totalCoins = ev.diamondCount * ev.repeatCount;
        Serial.printf(">>> GIFT: %d coins (diamond:%d x repeat:%d)\n",
                      totalCoins, ev.diamondCount, ev.repeatCount);

        // ── TIER 4: >= 20 coins → 6 bola melambung, kecepatan MAKSIMAL ──────────
        // Motor B full speed, Motor A lebih lambat → efek melambung kencang ke atas
        if (totalCoins >= 20) {
          pendingBalls  = 6;
          pendingSpeed  = SPEED_BOUNCE_A;   // motor atas: lebih lambat ≈ 2090
          pendingSpeedB = SPEED_BOUNCE_B;   // motor bawah: full speed (4095)
          pendingBounce = BOUNCE_MODE_ACTIVE;
          Serial.println("    → TIER 4 (>=20): 6 bola MELAMBUNG, kecepatan MAKSIMAL!");

        // ── TIER 3: >= 10 coins → 6 bola melambung, kecepatan normal ────────────
        // Motor B full speed, Motor A normal → efek melambung ke atas
        } else if (totalCoins >= 10) {
          pendingBalls  = 6;
          pendingSpeed  = SPEED_NORMAL;     // motor atas: 180 → normal
          pendingSpeedB = SPEED_BOUNCE_B;   // motor bawah: full speed (4095)
          pendingBounce = BOUNCE_MODE_ACTIVE;
          Serial.println("    → TIER 3 (>=10): 6 bola MELAMBUNG, kecepatan normal");

        // ── TIER 2: >= 5 coins → 3 bola melambung, kecepatan normal ─────────────
        // Motor B full speed, Motor A normal → efek melambung ke atas
        } else if (totalCoins >= 5) {
          pendingBalls  = 3;
          pendingSpeed  = SPEED_NORMAL;     // motor atas: 180 → normal
          pendingSpeedB = SPEED_BOUNCE_B;   // motor bawah: full speed (4095)
          pendingBounce = BOUNCE_MODE_ACTIVE;
          Serial.println("    → TIER 2 (>=5): 3 bola MELAMBUNG");

        // ── TIER 1: < 5 coins → 3 bola flat, kecepatan normal ───────────────────
        // Kedua motor kecepatan sama → bola lurus
        } else {
          pendingBalls  = 3;
          pendingSpeed  = SPEED_NORMAL;     // motor atas: 180 → normal
          pendingSpeedB = SPEED_NORMAL;     // motor bawah: sama → bola lurus
          pendingBounce = BOUNCE_MODE_NONE;
          Serial.printf("    → TIER 1 (<5 coins): 3 bola flat\n");
        }
      } else {
        return; // tipe event tidak dikenal
      }

      // Motor A & B diaktifkan BERSAMAAN via L298N — relay tidak ada hubungannya
      // Motor A (peluncur atas): GPIO 27/26/25 → L298N
      digitalWrite(IN1_PIN_A, HIGH);
      digitalWrite(IN2_PIN_A, LOW);
      ledcWrite(PWM_PIN_A, pendingSpeed);
      Serial.printf("[Motor A] ON via L298N (GPIO 27/26/25) PWM=%lu\n", pendingSpeed);
      // Motor B (peluncur bawah): GPIO 33/32/14 → L298N — aktif bersamaan dengan Motor A
      digitalWrite(IN3_PIN_B, HIGH);
      digitalWrite(IN4_PIN_B, LOW);
      ledcWrite(PWM_PIN_B, pendingSpeedB);
      Serial.printf("[Motor B] ON via L298N (GPIO 33/32/14) PWM=%lu\n", pendingSpeedB);
      // Relay (GPIO 4) belum disentuh — akan ON sendiri di PHASE_STORAGE_RUN untuk feeder

      phaseStartMs = now;
      motorPhase   = PHASE_LAUNCHER_SPOOLUP;
      break;
    }

    // ── LAUNCHER SPOOLUP: tunggu motor A stabil, lalu aktifkan motor B ──────
    // Motor A sudah spool-up sejak PHASE_IDLE via L298N (GPIO 27, 26, 25).
    // Motor B diaktifkan di sini setelah spoolup selesai via L298N (GPIO 33, 32, 14).
    // RELAY (GPIO 4) SAMA SEKALI TIDAK DISENTUH di fase ini.
    // Relay hanya diaktifkan di PHASE_STORAGE_RUN, terpisah total dari motor A & B.
    case PHASE_LAUNCHER_SPOOLUP: {
      if ((now - phaseStartMs) >= (uint32_t)LAUNCHER_SPOOLUP_MS) {
        if (pendingBounce == BOUNCE_MODE_ACTIVE) {
          // Bounce mode: tahan motor B dulu selama BOUNCE_DELAY_MS
          // Motor A tetap jalan, relay belum disentuh sama sekali
          Serial.println("[Spoolup] Selesai → bounce mode: masuk BOUNCE_DELAY");
          phaseStartMs = now;
          motorPhase   = PHASE_BOUNCE_DELAY;
        } else {
          // Mode normal: Motor A & B sudah jalan sejak PHASE_IDLE
          // Langsung lanjut ke PHASE_STORAGE_RUN untuk aktifkan feeder (relay)
          Serial.println("[Spoolup] Selesai → Motor A & B sudah jalan, lanjut STORAGE_RUN");
          phaseStartMs = now;
          motorPhase   = PHASE_STORAGE_RUN;
        }
      }
      break;
    }

    // ── BOUNCE DELAY: tunggu sebentar sebelum feeder aktif ──────────────
    // Motor A & B sudah jalan sejak PHASE_IDLE via L298N — tidak ada yang perlu diaktifkan.
    // Relay belum disentuh. Setelah delay, lanjut PHASE_STORAGE_RUN untuk aktifkan feeder.
    case PHASE_BOUNCE_DELAY: {
      if ((now - phaseStartMs) >= (uint32_t)BOUNCE_DELAY_MS) {
        Serial.println("[Bounce delay] Selesai → lanjut STORAGE_RUN, feeder akan aktif");
        phaseStartMs = now;
        motorPhase   = PHASE_STORAGE_RUN;
      }
      break;
    }

    // ── STORAGE RUN: relay (feeder) ON di sini, terpisah dari motor A & B ─
    // Motor A (GPIO 27/26/25) dan Motor B (GPIO 33/32/14) sudah jalan sejak fase sebelumnya.
    // Relay (GPIO 4) hanya mengontrol motor feeder — tidak ada hubungan dengan motor A/B.
    case PHASE_STORAGE_RUN: {
      uint32_t storageDuration = (uint32_t)pendingBalls * STORAGE_PER_BALL_MS;

      // Aktifkan feeder (relay GPIO 4) di awal fase ini jika belum aktif
      // Ini independen dari motor A & B yang sudah jalan via L298N
      if (!relayActive) {
        digitalWrite(RELAY_PIN, LOW);   // feeder ON (active LOW)
        relayActive = true;
        Serial.println("[RELAY] ON (GPIO 4) → feeder aktif, dorong bola");
      }

      if ((now - phaseStartMs) >= storageDuration) {
        stopSemuaMotor();
        Serial.printf("[Motor] Selesai. %d bola x %dms = %dms feeder aktif\n",
                      pendingBalls, STORAGE_PER_BALL_MS, storageDuration);
        phaseStartMs = now;
        motorPhase   = PHASE_DONE;
      }
      break;
    }

    // ── DONE: periksa apakah ada event lagi di stack ──────────────────────
    case PHASE_DONE: {
      if (eventStack[readIdx].filled) {
        Serial.println("[Motor] Stack masih ada — lanjut slot berikutnya.");
      }
      motorPhase = PHASE_IDLE;
      break;
    }
  }
}

// ─── Helper: hash ringan ──────────────────────────────────────────────────────
uint32_t hashPayload(const char* buf, unsigned int len) {
  uint32_t h = 0;
  for (unsigned int i = 0; i < len; i++) h = h * 31 + (uint8_t)buf[i];
  return h;
}

// ─── Fallback: push pending gift jika repeatEnd=true tidak datang dalam GIFT_FALLBACK_MS ─
// Dipanggil setiap loop(). Menangani gift non-combo (flower/1coin) yang server
// hanya kirim repeatEnd=false tanpa repeatEnd=true.
void checkPendingGift() {
  if (!pendingGift.valid) return;
  uint32_t now = millis();
  if ((now - pendingGift.receivedAt) >= (uint32_t)GIFT_FALLBACK_MS) {
    Serial.printf("[GIFT] Fallback: repeatEnd=true tidak datang → push pending (diamond:%d x repeat:%d)\n",
                  pendingGift.diamondCount, pendingGift.repeatCount);
    pushEvent(EVENT_GIFT, pendingGift.diamondCount, pendingGift.repeatCount, 0, "FALLBACK");
    lastPushedGiftHash   = pendingGift.hash;
    lastPushedGiftTimeMs = now;
    pendingGift.valid    = false;
  }
}

// ─── Helper: kirim GET request HTTPS ke endpoint server ──────────────────────
bool httpsGet(const char* url) {
  WiFiClientSecure httpClient;
  httpClient.setInsecure();
  HTTPClient http;
  http.begin(httpClient, url);
  int code = http.GET();
  http.end();
  if (code > 0) {
    Serial.printf("  HTTP %d\n", code);
    return (code == 200);
  }
  Serial.printf("  Gagal (code: %d)\n", code);
  return false;
}

// ─── Panggil /tiktok-connect/:device/:username di server ─────────────────────
void connectTiktok() {
  if (tiktokUsername[0] == '\0') {
    Serial.println("[TikTok] Username belum diset, buka portal konfigurasi.");
    return;
  }
  static char url[160];
  snprintf(url, sizeof(url), "%s%s/tiktok-connect/%s/%s",
           SERVER_HOST, SERVER_PATH, MQTT_CLIENT_ID, tiktokUsername);
  Serial.printf("[TikTok] Menghubungkan ke %s\n", url);
  tiktokConnected = httpsGet(url);
}

// ─── Panggil /tiktok-disconnect/:username di server ───────────────────────────
void disconnectTiktok() {
  if (tiktokUsername[0] == '\0') return;
  static char url[160];
  snprintf(url, sizeof(url), "%s%s/tiktok-disconnect/%s",
           SERVER_HOST, SERVER_PATH, tiktokUsername);
  Serial.printf("[TikTok] Memutus koneksi ke %s\n", url);
  httpsGet(url);
  tiktokConnected = false;
}

// ─── Buka ulang portal WiFiManager untuk rekonfigurasi ───────────────────────
void openConfigPortal() {
  Serial.println("[CFG] Membuka portal konfigurasi...");
  if (tiktokConnected) disconnectTiktok();

  WiFiManager wm;
  WiFiManagerParameter paramUsername("tiktok_user", "TikTok Username", tiktokUsername, 63);
  wm.addParameter(&paramUsername);
  wm.setConfigPortalTimeout(180);
  wm.startConfigPortal(WIFIMANAGER_AP_NAME);

  const char* newUser = paramUsername.getValue();
  if (newUser[0] != '\0') {
    strlcpy(tiktokUsername, newUser, sizeof(tiktokUsername));
    prefs.begin("feeder", false);
    prefs.putString("tiktok_user", tiktokUsername);
    prefs.end();
    Serial.printf("[CFG] Username disimpan: %s\n", tiktokUsername);
  }
  ESP.restart();
}

// ─── Cek tombol: short press = toggle TikTok, long press = buka portal ────────
void handleButton() {
  bool reading = digitalRead(BTN_PIN);

  if (reading != lastBtnState) {
    lastDebounceTime = millis();
    if (reading == LOW) btnPressStart = millis();
  }

  if ((millis() - lastDebounceTime) >= DEBOUNCE_MS) {
    if (reading == LOW && (millis() - btnPressStart) >= LONG_PRESS_MS) {
      openConfigPortal();
      return;
    }
    if (reading == HIGH && lastBtnState == LOW) {
      if ((millis() - btnPressStart) < LONG_PRESS_MS) {
        if (tiktokConnected) {
          disconnectTiktok();
          Serial.println("[BTN] TikTok disconnected.");
        } else {
          connectTiktok();
          Serial.println("[BTN] TikTok connected.");
        }
      }
    }
  }
  lastBtnState = reading;
}

// ─── Koneksi WiFi via WiFiManager ─────────────────────────────────────────────
void connectWifi() {
  WiFiManager wm;
  WiFiManagerParameter paramUsername("tiktok_user", "TikTok Username", tiktokUsername, 63);
  wm.addParameter(&paramUsername);
  wm.setSaveParamsCallback([&]() {
    const char* newUser = paramUsername.getValue();
    if (newUser[0] != '\0') {
      strlcpy(tiktokUsername, newUser, sizeof(tiktokUsername));
      prefs.begin("feeder", false);
      prefs.putString("tiktok_user", tiktokUsername);
      prefs.end();
      Serial.printf("[CFG] Username disimpan: %s\n", tiktokUsername);
    }
  });
  bool connected = wm.autoConnect(WIFIMANAGER_AP_NAME);
  if (!connected) {
    Serial.println("[WiFi] Gagal terhubung, restart...");
    ESP.restart();
  }
  Serial.print("[WiFi] Terhubung. IP: ");
  Serial.println(WiFi.localIP());
}

// ─── Koneksi & subscribe MQTT (non-blocking) ─────────────────────────────────
void connectMqtt() {
  if (mqttClient.connected()) return;
  Serial.print("[MQTT] Menghubungkan...");
  if (mqttClient.connect(MQTT_CLIENT_ID, MQTT_USERNAME, MQTT_PASSWORD)) {
    Serial.println(" terhubung.");
    mqttClient.subscribe(MQTT_TOPIC);
    Serial.printf("[MQTT] Subscribe ke: %s\n", MQTT_TOPIC);
  } else {
    Serial.printf(" gagal (state=%d)\n", mqttClient.state());
  }
}

// ─── Callback: dipanggil setiap ada pesan MQTT masuk ─────────────────────────
void onMqttMessage(char* topic, byte* payload, unsigned int length) {
  Serial.printf("\n[MQTT] Pesan diterima di topic: %s\n", topic);

  static char buffer[1024];
  if (length >= sizeof(buffer)) {
    Serial.println("[MQTT] Payload terlalu besar, dilewati.");
    return;
  }
  memcpy(buffer, payload, length);
  buffer[length] = '\0';

  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, buffer);
  if (err) {
    Serial.printf("[JSON] Gagal parse: %s\n", err.c_str());
    return;
  }

  const char* streamer = doc["requestor"] | "";
  const char* type     = doc["type"]      | "";
  int eventType        = EVENT_IDLE;
  int diamondCount     = 0;
  int repeatCount      = 0;
  int likeCount        = 0;

  Serial.printf("  streamer : %s\n", streamer);
  Serial.printf("  type     : %s\n", type);

  if (strcmp(type, "roomUser") == 0) {
    int viewerCount = doc["viewerCount"] | 0;
    int totalUser   = doc["totalUser"]   | 0;
    Serial.printf("  viewerCount : %d\n", viewerCount);
    Serial.printf("  totalUser   : %d\n", totalUser);

  } else if (strcmp(type, "gift") == 0) {
    eventType             = EVENT_GIFT;
    const char* uniqueId  = doc["uniqueId"]    | "";
    const char* nickname  = doc["nickname"]    | "";
    int giftId            = doc["giftId"]      | 0;
    repeatCount           = doc["repeatCount"] | 0;
    bool repeatEnd        = doc["repeatEnd"]   | false;
    const char* giftName  = doc["giftName"]    | "";
    diamondCount          = doc["diamondCount"]| 0;
    Serial.printf("  uniqueId     : %s\n", uniqueId);
    Serial.printf("  nickname     : %s\n", nickname);
    Serial.printf("  giftId       : %d\n", giftId);
    Serial.printf("  giftName     : %s\n", giftName);
    Serial.printf("  diamondCount : %d\n", diamondCount);
    Serial.printf("  repeatCount  : %d\n", repeatCount);
    Serial.printf("  repeatEnd    : %s\n", repeatEnd ? "true" : "false");

    // ── Dedup gift ────────────────────────────────────────────────────────────
    // STRATEGI: pending + fallback timer
    //
    // Server TikTok kirim 2 paket per gift:
    //   Paket A: repeatEnd=false  → simpan sebagai "pending", JANGAN push dulu
    //   Paket B: repeatEnd=true   → ini event final, push ke stack, hapus pending
    //
    // Untuk gift non-combo (e.g. flower/1coin) server terkadang HANYA kirim
    // repeatEnd=false tanpa ada repeatEnd=true → ditangani oleh checkPendingGift()
    // di loop() yang push setelah GIFT_FALLBACK_MS (600ms) tidak ada repeatEnd=true.
    //
    // Hash: giftId + uniqueId (tanpa repeatCount) agar A & B punya hash yang sama.

    static char dedupBuf[128];
    snprintf(dedupBuf, sizeof(dedupBuf), "%d|%s", giftId, uniqueId);
    uint32_t thisHash = hashPayload(dedupBuf, strlen(dedupBuf));
    uint32_t nowMs    = millis();

    // ── Cooldown: tolak gift yang sama dalam 5 detik setelah di-push ─────────
    // Menangani server TikTok yang kirim 2 event identik dengan jeda hingga 3+ detik
    if (thisHash == lastPushedGiftHash && (nowMs - lastPushedGiftTimeMs) < GIFT_COOLDOWN_MS) {
      Serial.printf("    [GIFT] Cooldown aktif, dilewati (jarak dari push terakhir: %lums)\n",
                    nowMs - lastPushedGiftTimeMs);
      return;
    }

    // ── Dedup level 2: tolak paket duplikat cepat dalam GIFT_DEDUP_MS ──────────
    if (thisHash == lastGiftHash && (nowMs - lastGiftHashTime) < GIFT_DEDUP_MS) {
      Serial.printf("    [GIFT] Duplikat MQTT dilewati (jarak: %lums)\n", nowMs - lastGiftHashTime);
      return;
    }
    lastGiftHash     = thisHash;
    lastGiftHashTime = nowMs;

    if (!repeatEnd) {
      // Paket A (repeatEnd=false): simpan sebagai pending, JANGAN push
      if (pendingGift.valid && pendingGift.hash == thisHash) {
        // Pending dengan hash sama sudah ada → update repeatCount saja (combo progress)
        Serial.printf("    [GIFT] Combo progress → update pending repeatCount=%d\n", repeatCount);
        pendingGift.repeatCount = repeatCount;
        pendingGift.receivedAt  = nowMs;
      } else {
        // Gift baru atau hash berbeda → simpan pending baru
        pendingGift.valid        = true;
        pendingGift.hash         = thisHash;
        pendingGift.receivedAt   = nowMs;
        pendingGift.diamondCount = diamondCount;
        pendingGift.repeatCount  = repeatCount;
        Serial.printf("    [GIFT] Pending disimpan (fallback %dms)\n", GIFT_FALLBACK_MS);
      }
      return;   // JANGAN push di sini

    } else {
      // Paket B (repeatEnd=true): event final → push langsung
      int d = pendingGift.valid ? pendingGift.diamondCount : diamondCount;
      pendingGift.valid = false;
      Serial.println("    [GIFT] repeatEnd=true → push ke stack");
      pushEvent(EVENT_GIFT, d, repeatCount, 0, "REPEAT_END");
      lastPushedGiftHash   = thisHash;
      lastPushedGiftTimeMs = nowMs;
      return;   // WAJIB: cegah pushEvent generik di bawah
    }

  } else if (strcmp(type, "like") == 0) {
    eventType             = EVENT_LIKE;
    const char* uniqueId  = doc["uniqueId"]       | "";
    const char* nickname  = doc["nickname"]       | "";
    likeCount             = doc["likeCount"]      | 0;
    int totalLikeCount    = doc["totalLikeCount"] | 0;
    Serial.printf("  uniqueId       : %s\n", uniqueId);
    Serial.printf("  nickname       : %s\n", nickname);
    Serial.printf("  likeCount      : %d\n", likeCount);
    Serial.printf("  totalLikeCount : %d\n", totalLikeCount);

    // FIX #1: Dedup like — hash dari uniqueId+likeCount (bukan full buffer)
    // Full buffer berisi field lain yang bisa berubah tiap paket → hash selalu beda → dedup gagal
    static char likeDedupBuf[128];
    snprintf(likeDedupBuf, sizeof(likeDedupBuf), "%s|%d", uniqueId, likeCount);
    uint32_t thisHash = hashPayload(likeDedupBuf, strlen(likeDedupBuf));
    uint32_t nowMs    = millis();
    if (thisHash == lastLikeHash && (nowMs - lastLikeHashTime) < LIKE_DEDUP_MS) {
      Serial.printf("    [LIKE] Duplikat dilewati (jarak: %lums)\n", nowMs - lastLikeHashTime);
      return;
    }
    lastLikeHash     = thisHash;
    lastLikeHashTime = nowMs;

  } else if (strcmp(type, "chat") == 0) {
    eventType             = EVENT_CHAT;
    const char* uniqueId  = doc["uniqueId"] | "";
    const char* nickname  = doc["nickname"] | "";
    const char* comment   = doc["comment"]  | "";
    Serial.printf("  uniqueId : %s\n", uniqueId);
    Serial.printf("  nickname : %s\n", nickname);
    Serial.printf("  comment  : %s\n", comment);

  } else {
    if (strcmp(type, "follow") == 0) {
      eventType = EVENT_FOLLOW;
    } else if (strcmp(type, "share") == 0) {
      eventType = EVENT_SHARE;
    }
    const char* uniqueId = doc["uniqueId"] | "";
    const char* nickname = doc["nickname"] | "";
    Serial.printf("  uniqueId : %s\n", uniqueId);
    Serial.printf("  nickname : %s\n", nickname);
  }

  // Push ke stack: LIKE saja — GIFT sudah di-push langsung di blok gift (repeatEnd=true / fallback)
  // eventType == EVENT_GIFT tidak akan pernah sampai sini karena semua path gift pakai return
  if (eventType == EVENT_LIKE) {
    pushEvent(eventType, diamondCount, repeatCount, likeCount, "LIKE");
  }
}

// ─── Setup ────────────────────────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);

  pinMode(BTN_PIN,   INPUT_PULLUP);
  pinMode(RELAY_PIN, OUTPUT);
  pinMode(IN1_PIN_A, OUTPUT);
  pinMode(IN2_PIN_A, OUTPUT);
  pinMode(IN3_PIN_B, OUTPUT);
  pinMode(IN4_PIN_B, OUTPUT);

  // Semua output awal LOW kecuali relay (active LOW → HIGH = OFF)
  digitalWrite(IN1_PIN_A, LOW);
  digitalWrite(IN2_PIN_A, LOW);
  digitalWrite(IN3_PIN_B, LOW);
  digitalWrite(IN4_PIN_B, LOW);
  digitalWrite(RELAY_PIN, HIGH);   // relay OFF saat boot

  // PWM motor
  ledcAttach(PWM_PIN_A, PWM_FREQ, PWM_RESOLUTION);
  ledcAttach(PWM_PIN_B, PWM_FREQ, PWM_RESOLUTION);
  ledcWrite(PWM_PIN_A, 0);
  ledcWrite(PWM_PIN_B, 0);
  motorReady = true;
  Serial.println("[Motor] PWM siap.");

  // Baca username TikTok yang tersimpan di NVS
  prefs.begin("feeder", true);
  prefs.getString("tiktok_user", tiktokUsername, sizeof(tiktokUsername));
  prefs.end();
  if (tiktokUsername[0] != '\0') {
    Serial.printf("[NVS] Username dimuat: %s\n", tiktokUsername);
  } else {
    Serial.println("[NVS] Username belum diset — isi via portal konfigurasi.");
  }

  wifiClient.setInsecure();
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setBufferSize(1024);
  mqttClient.setCallback(onMqttMessage);

  connectWifi();
  connectTiktok();
  connectMqtt();
}

// ─── Loop ─────────────────────────────────────────────────────────────────────
void loop() {
  handleButton();
  checkPendingGift();   // fallback: push gift non-combo jika repeatEnd=true tidak datang
  processMotor();       // baca stack → gerakkan motor (non-blocking)
  // trackRelay() dihapus — relay dikelola langsung di processMotor (FIX #3)

  uint32_t now = millis();
  if (!mqttClient.connected() && (now - lastMqttAttempt) >= MQTT_RETRY_MS) {
    lastMqttAttempt = now;
    connectMqtt();
  }
  mqttClient.loop();
}
