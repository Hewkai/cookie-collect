from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, urljoin
import time
import random
import mysql.connector
from datetime import datetime, timezone
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
import csv
import os
import threading


# -----------------------
# Database helper
# -----------------------
def get_db():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cookies_db"
    )
    cursor = db.cursor(dictionary=True)
    return db, cursor


def init_db():
    db, cursor = get_db()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cookies (
        id INT AUTO_INCREMENT PRIMARY KEY,
        website VARCHAR(255),
        name VARCHAR(255),
        value TEXT,
        domain VARCHAR(255),
        path VARCHAR(255),
        expires VARCHAR(50),
        httponly VARCHAR(3),
        action_type VARCHAR(20),
        is_api_store BOOLEAN NULL,
        samesite VARCHAR(20) NULL,
        https BOOLEAN NULL,
        collected_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)
    db.commit()
    cursor.close()
    db.close()


# -----------------------
# Chrome driver
# -----------------------
def create_driver(user_data_dir: Path | None):
    seleniumwire_options = {
        'disable_encoding': True,
    'ignore_encoding_errors': True,
    'request_storage_base_dir': None,
    'proxy': {
        'http2': False        # <--- THE REAL FIX
    },
    'port': 0
    }
    # options = {'disable_encoding': True}
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--lang=en-GB")

    # ถ้าให้ใช้โปรไฟล์ custom (เช่น Cookies) ค่อยใส่ user-data-dir
    if user_data_dir is not None:
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")

    driver = webdriver.Chrome(options=chrome_options, 
                              seleniumwire_options=seleniumwire_options)

    # JS hook document.cookie
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            (function() {
                const originalSetCookie = document.__lookupSetter__('cookie');
                if (!window.jsCookies) window.jsCookies = {};

                Object.defineProperty(document, "cookie", {
                    set: function(value) {
                        try {
                            const parts = value.split(";").map(p => p.trim());
                            const [name, val] = parts[0].split("=");
                            const now = new Date();
                            const setTime = now.toISOString();
                            let expireSeconds = "never";
                            let samesite = "Unspecified"; 

                            parts.slice(1).forEach(p => {
                                if (p.toLowerCase().startsWith("expires=")) {
                                    const expDate = new Date(p.slice(8));
                                    expireSeconds = Math.floor((expDate - now)/1000);
                                } else if (p.toLowerCase().startsWith("max-age=")) {
                                    const seconds = parseInt(p.slice(8));
                                    if (!isNaN(seconds)) expireSeconds = seconds;
                                } else if (p.toLowerCase().startsWith("samesite=")) {
                                    let ss = p.slice(9).toLowerCase();
                                    if (ss === "lax") samesite = "Lax";
                                    else if (ss === "strict") samesite = "Strict";
                                    else if (ss === "none") samesite = "None"; 
                                    else samesite = ss; 
                                }
                            });

                            let action;
                            if (!window.jsCookies.hasOwnProperty(name)) {
                                action = "add";       
                            } else if (val === "" || expireSeconds === 0) {
                                action = "delete";    
                            } else {
                                action = "edit"; 
                            }

                            window.jsCookies[name] = {
                                name: name,
                                value: val || "",
                                set_time: setTime,
                                expires: (val === "" || expireSeconds === 0) ? 0 : expireSeconds,
                                samesite: samesite,   
                                action: action,
                                from: "Document"
                            };
                        } catch(e) {
                            console.log("Cookie hook error", e);
                        }
                        if(originalSetCookie) originalSetCookie.call(document, value);
                    },
                    get: function() { return document.cookie; }
                });

            })();
        """
    })
    return driver


# -----------------------
# Save cookies
# -----------------------
def save_cookies(db, cursor, site, cookies):
    for c in cookies:
        domain = c.get('domain', '').lstrip('.').replace("www.", "")
        path = c.get('path', '/')
        httponly = c.get('httponly', 'No')
        expires = c.get('expires')
        samesite = c.get('samesite', 'Unspecified')

        # Convert expires to integer timestamp if possible
        try:
            expires_ts = int(expires)
        except (TypeError, ValueError):
            expires_ts = None

        cursor.execute("""
            SELECT name, value, domain, path, website, expires, httponly, samesite, action_type, is_api_store
            FROM cookies
            WHERE name=%s
            AND domain=%s
            AND path=%s
            AND website=%s
            AND value=%s
            AND httponly=%s
            AND samesite=%s
            AND action_type=%s
            AND is_api_store=%s
            ORDER BY last_seen DESC
            LIMIT 1
        """, (
            c['name'],
            domain,
            path,
            site,
            c['value'],
            httponly,
            samesite,
            c.get('action_type', 'unknown'),
            c.get('is_api_store'),
        ))

        row = cursor.fetchone()
        skip_insert = False

        if row:
            try:
                existing_expires_ts = int(row['expires'])
            except (TypeError, ValueError):
                existing_expires_ts = None

            if expires_ts is not None and existing_expires_ts is not None:
                diff = abs(existing_expires_ts - expires_ts)
                if diff <= 100:
                    skip_insert = True
            else:
                skip_insert = True

        if skip_insert:
            continue

        try:
            collected_at = c['collected_at']
            if isinstance(collected_at, str):
                collected_at = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
        except Exception:
            collected_at = datetime.now(timezone.utc)

        cursor.execute("""
            INSERT INTO cookies (website, name, value, domain, path, expires, httponly, samesite, action_type, is_api_store, collected_at, https)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            site,
            c['name'],
            c['value'],
            domain,
            path,
            expires,
            httponly,
            samesite,
            c.get('action_type', 'unknown'),
            c.get('is_api_store'),
            collected_at,
            c.get('https')
        ))

    db.commit()


def normalize_domain(netloc):
    return netloc.lower().lstrip("www.")


def track_cookies(driver, interval=1, stable_seconds=3, timeout=30):
    driver.execute_script("""
        if (!window.jsCookies) window.jsCookies = {};
        if (!window.lastSnapshot) window.lastSnapshot = {};

        window.getCookieDiff = async function() {
            let result = [];
            try {
                const cookies = window.cookieStore ? await window.cookieStore.getAll() : [];
                const now = new Date().toISOString();
                let current = {};
                cookies.forEach(c => { if(c && c.name) current[c.name] = c; });

                // Detect add/edit
                for (const [name, c] of Object.entries(current)) {
                    if(!c) continue;

                    let samesite = "Unspecified";
                    if (c.sameSite) {
                        let ss = c.sameSite.toLowerCase();
                        if (ss === "lax") samesite = "Lax";
                        else if (ss === "strict") samesite = "Strict";
                        else if (ss === "none") samesite = "None";
                        else samesite = c.sameSite;
                    }

                    let action = "add";
                    if(window.lastSnapshot[name]) {
                        let old = window.lastSnapshot[name];
                        if(old) {
                            if(old.value !== c.value ||
                               old.expires !== c.expires ||
                               old.path !== c.path ||
                               old.domain !== c.domain ||
                               old.samesite !== samesite) {
                                action = "edit";
                            } else {
                                continue;
                            }
                        }
                    }

                    window.jsCookies[name] = {
                        name: name,
                        value: c.value || "",
                        domain: c.domain || window.location.hostname,
                        path: c.path || "/",
                        set_time: now,
                        expires: c.expires ? Math.floor((new Date(c.expires) - new Date())/1000) : "never",
                        samesite: samesite,
                        action: action,
                        from: "cookieStore"
                    };
                    result.push(window.jsCookies[name]);
                }

                // Detect delete
                for (const [name, old] of Object.entries(window.lastSnapshot)) {
                    if(!current[name]) {
                        window.jsCookies[name] = {
                            name: name,
                            value: "",
                            domain: old.domain || window.location.hostname,
                            path: old.path || "/",
                            set_time: now,
                            expires: 0,
                            samesite: old.samesite || "Unspecified",
                            action: "delete",
                            from: "cookieStore"
                        };
                        result.push(window.jsCookies[name]);
                    }
                }

                window.lastSnapshot = current;
            } catch(e) { result.push({error: e.toString()}); }
            return result;
        };
    """)

    start_time = time.time()
    last_change_time = time.time()
    final_changes = []

    while True:
        if time.time() - start_time > timeout:
            break

        changes = driver.execute_async_script("""
            const done = arguments[0];
            window.getCookieDiff().then(done).catch(e => done([{error: e.toString()}]));
        """)

        if changes and isinstance(changes, list) and len(changes) > 0:
            for c in changes:
                if c and isinstance(c, dict):
                    final_changes.append(c)
            last_change_time = time.time()

        if time.time() - last_change_time >= stable_seconds:
            break

        time.sleep(interval)

    return final_changes


# -----------------------
# Worker
# -----------------------
def crawl_with_profile(
    name: str,
    websites,
    start_index: int,
    end_index: int,
    user_data_dir: Path | None,
    progress_file: str
):
    print(f"[{name}] Starting crawler from index {start_index} to {end_index}")

    # load progress
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            content = f.read().strip()
            if content.isdigit():
                saved_index = int(content)
                if saved_index > start_index:
                    start_index = saved_index
                    print(f"[{name}] 🔁 Resume from index {start_index}")

    db, cursor = get_db()
    driver = create_driver(user_data_dir)

    try:
        for i in range(start_index, min(end_index, len(websites))):
            site = websites[i]
            print(f"\n[{name}] 🌍 [{i+1}/{len(websites)}] Loading site: {site}")

            try:
                base_domain = urlparse(site).netloc
                driver.execute_script("window.jsCookies = {};")
                driver.get(site)
                time.sleep(3)
                max_pages = 6
                scroll_pause_time = 1
                pages_visited = 0

                all_cookies = []
                setup_time = datetime.now(timezone.utc)
                supports_cookie_store = driver.execute_script("return 'cookieStore' in window;")
                if supports_cookie_store:
                    print(f"[{name}] ✅ cookieStore API is available")
                    cookies = track_cookies(driver)
                else:
                    print(f"[{name}] ⚠ cookieStore API is NOT available")

                # JS cookies (first page)
                try:
                    js_cookies = driver.execute_script("return Object.values(window.jsCookies || {})")
                    for c in js_cookies:
                        domain = c.get('domain') or base_domain
                        expires = c.get('expires') or "never"
                        samesite = c.get('samesite') or "Unspecified"
                        all_cookies.append({
                            "name": c['name'],
                            "value": c['value'],
                            "domain": domain,
                            "path": "/",
                            "expires": expires,
                            "httponly": "No",
                            "samesite": samesite,
                            "action_type": "js-set:" + c['action'],
                            "is_api_store": True if c['from'] == "cookieStore" else False,
                            "collected_at": c['set_time']
                        })
                except Exception as e:
                    print(f"[{name}] Error fetching JS cookies:", e)

                # internal navigation
                while pages_visited < max_pages:
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    while True:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(scroll_pause_time)
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height

                    try:
                        if pages_visited + 1 < max_pages:
                            links = driver.find_elements(By.TAG_NAME, "a")
                            internal_links = []
                            for l in links:
                                href = l.get_attribute("href")
                                if href:
                                    full_url = urljoin(f"https://{base_domain}", href)
                                    parsed_href = urlparse(full_url)
                                    if normalize_domain(parsed_href.netloc) == normalize_domain(base_domain):
                                        internal_links.append(full_url)
                            if internal_links:
                                link_to_go = random.choice(internal_links)
                                driver.get(link_to_go)
                                WebDriverWait(driver, 10).until(
                                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                                )
                                pages_visited += 1
                                time.sleep(scroll_pause_time + random.random() * 2)
                                supports_cookie_store = driver.execute_script("return 'cookieStore' in window;")
                                if supports_cookie_store:
                                    print(f"[{name}] ✅ cookieStore API is available (inner)")
                                    cookies = track_cookies(driver)
                                else:
                                    print(f"[{name}] ⚠ cookieStore API is NOT available (inner)")
                                try:
                                    js_cookies = driver.execute_script("return Object.values(window.jsCookies || {})")
                                    for c in js_cookies:
                                        domain = c.get('domain') or base_domain
                                        expires = c.get('expires') or "never"
                                        samesite = c.get('samesite') or "Unspecified"
                                        all_cookies.append({
                                            "name": c['name'],
                                            "value": c['value'],
                                            "domain": domain,
                                            "path": "/",
                                            "expires": expires,
                                            "httponly": "No",
                                            "samesite": samesite,
                                            "action_type": "js-set:" + c['action'],
                                            "is_api_store": True if c['from'] == "cookieStore" else False,
                                            "collected_at": c['set_time']
                                        })
                                except Exception as e:
                                    print(f"[{name}] Error fetching JS cookies (inner):", e)
                            else:
                                break
                        else:
                            pages_visited += 1
                    except WebDriverException as e:
                        print(f"[{name}] WebDriverException occurred:", e)

                # Network cookies
                for request in driver.requests:
                    if request.response and 'Set-Cookie' in request.response.headers:
                        is_https = 1 if request.url.lower().startswith("https://") else 0
                        request_time = request.date
                        server_time = request.response.headers.get('Date')
                        if server_time:
                            from email.utils import parsedate_to_datetime
                            try:
                                server_time_dt = parsedate_to_datetime(server_time)
                            except:
                                server_time_dt = request_time
                        else:
                            server_time_dt = request_time

                        cookie_headers = request.response.headers.get_all('Set-Cookie') if hasattr(request.response.headers, 'get_all') else [request.response.headers['Set-Cookie']]
                        for cookie_str in cookie_headers:
                            parts = cookie_str.split(';')
                            name_value = parts[0].split('=')
                            name = name_value[0]
                            value = name_value[1] if len(name_value) > 1 else ""
                            domain = base_domain
                            path = "/"
                            httponly = "Yes" if "HttpOnly" in cookie_str else "No"
                            expires = "never"
                            samesite = "Unspecified"

                            for p in parts[1:]:
                                p = p.strip()
                                if p.lower().startswith("domain="):
                                    domain = p[7:]
                                elif p.lower().startswith("path="):
                                    path = p[5:]
                                elif p.lower().startswith("expires="):
                                    expires = p[8:]
                                elif p.lower().startswith("samesite="):
                                    samesite = p[9:].capitalize()

                            if expires and expires != "never":
                                from email.utils import parsedate_to_datetime
                                try:
                                    expires_dt = parsedate_to_datetime(expires)
                                    expire_seconds = int((expires_dt - server_time_dt).total_seconds())
                                    if expire_seconds < 0:
                                        expire_seconds = 0
                                except Exception:
                                    expire_seconds = "never"
                            else:
                                expire_seconds = "never"

                            all_cookies.append({
                                "name": name,
                                "value": value,
                                "domain": domain,
                                "path": path,
                                "expires": expire_seconds,
                                "httponly": httponly,
                                "samesite": samesite,
                                "action_type": "network:add",
                                "collected_at": server_time_dt,
                                "https": is_https
                            })

                # Save to DB
                save_cookies(db, cursor, site, all_cookies)
                driver.requests.clear()

                # Save progress
                with open(progress_file, "w") as f:
                    f.write(str(i + 1))

            except Exception as e:
                print(f"[{name}] ❌ Error processing {site}: {e}")
                continue

        print(f"[{name}] ✅ Finished range {start_index} - {end_index}")
    finally:
        driver.quit()
        cursor.close()
        db.close()


# -----------------------
# Main
# -----------------------
if __name__ == "__main__":
    init_db()

    # โหลดเว็บไซต์จาก CSV
    websites = []
    with open('accessible_websites_merged_final.csv', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0].isdigit():
                domain = row[1].strip()
                if not domain.startswith("http"):
                    domain = "https://" + domain
                websites.append(domain)

    print(f"Loaded {len(websites)} websites from CSV.")

    # base path ของ Chrome
    base_user_data = Path.home() / "AppData" / "Local" / "Google" / "Chrome" / "User Data"

    # Profile 1: ใช้โปรไฟล์ Cookies, เริ่มจาก index 10880
    profile1_dir = base_user_data / "Cookies"       # <--- โปรไฟล์ Cookies
    start_index_1 = 0
    end_index_1 = 2718
    progress_file_1 = "progress_profile1.txt"

    # Profile 2: ใช้ default (ไม่กำหนด user-data-dir), เริ่มจาก index 20000
    profile2_dir = base_user_data / "Cookies2"                             # <--- Cookies2 profile
    start_index_2 = 5439
    end_index_2 = 8160
    progress_file_2 = "progress_profile2.txt"

    # Thread ทั้งสอง
    t1 = threading.Thread(
        target=crawl_with_profile,
        args=("Profile-1-Cookies", websites, start_index_1, end_index_1, profile1_dir, progress_file_1),
        daemon=True
    )
    t2 = threading.Thread(
        target=crawl_with_profile,
        args=("Profile-2-Cookies2", websites, start_index_2, end_index_2, profile2_dir, progress_file_2),
        daemon=True
    )

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print("🎉 All profiles finished crawling.")
