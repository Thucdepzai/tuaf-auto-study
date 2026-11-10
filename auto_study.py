import sys
import os
import re
import json
import time
import queue
import signal
import threading
import unicodedata
from datetime import datetime, timezone, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

def _load_config():
    cfg = {
        'TUAF_USERNAME': os.environ.get('TUAF_USERNAME', ''),
        'TUAF_PASSWORD': os.environ.get('TUAF_PASSWORD', ''),
        'ZALO_BOT_TOKEN': os.environ.get('ZALO_BOT_TOKEN', ''),
        'ZALO_CHAT_ID': os.environ.get('ZALO_CHAT_ID', ''),
    }
    secrets_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.secrets.json')
    if os.path.exists(secrets_file):
        try:
            with open(secrets_file, 'r', encoding='utf-8') as f:
                local_cfg = json.load(f)
            for k in cfg:
                if not cfg[k] and local_cfg.get(k):
                    cfg[k] = local_cfg[k]
        except Exception:
            pass
    return cfg

_CFG = _load_config()
USERNAME = _CFG['TUAF_USERNAME']
PASSWORD = _CFG['TUAF_PASSWORD']
ACTIVE_COURSE_IDS = [3651, 3573, 3481]
MIN_AUTO_BATCH = 261025  # Tự động thêm mọi môn mới từ đợt 261025 trở đi
ZALO_BOT_TOKEN = _CFG['ZALO_BOT_TOKEN']
ZALO_CHAT_ID = _CFG['ZALO_CHAT_ID']
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'auto_study.log')

VN_TZ = timezone(timedelta(hours=7))
STOP_REQUESTED = False

def _handle_stop_signal(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    print(f"Received signal {signum}, shutting down cleanly...", flush=True)
    os._exit(0)

signal.signal(signal.SIGTERM, _handle_stop_signal)
signal.signal(signal.SIGINT, _handle_stop_signal)

def _create_resilient_session():
    s = requests.Session()
    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=None,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    })
    return s

session = _create_resilient_session()
SCAN_LOCK = threading.Lock()

def log(msg):
    ts = datetime.now(VN_TZ).strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass

def safe_get(url, retries=3, timeout=20, **kwargs):
    for i in range(retries):
        try:
            return session.get(url, timeout=timeout, **kwargs)
        except Exception as e:
            if i == retries - 1:
                raise
            log(f"GET retry {i+1}/{retries} for {url}: {e}")
            time.sleep(0.6 * (i + 1))

def safe_post(url, retries=3, timeout=20, **kwargs):
    for i in range(retries):
        try:
            return session.post(url, timeout=timeout, **kwargs)
        except Exception as e:
            if i == retries - 1:
                raise
            log(f"POST retry {i+1}/{retries} for {url}: {e}")
            time.sleep(0.6 * (i + 1))

def strip_accents(s):
    if not s:
        return ''
    s = s.replace('đ', 'd').replace('Đ', 'D')
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').lower()

def send_zalo_notification(text, chat_id=None):
    target_chat = chat_id or ZALO_CHAT_ID
    if not ZALO_BOT_TOKEN or not target_chat:
        return
    try:
        url = f"https://bot-api.zaloplatforms.com/bot{ZALO_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": str(target_chat),
            "text": text[:1950]
        }, timeout=12)
        log(f"Zalo notification sent to {target_chat}: {resp.status_code}")
    except Exception as e:
        log(f"Failed to send Zalo notification: {e}")

def build_conversational_prefix(user_text, matched_new_courses=None):
    if not user_text:
        return ""
    if matched_new_courses:
        names = ", ".join(name for _, name in matched_new_courses)
        return f"💬 Bạn yêu cầu môn: \"{user_text}\"\n🎯 Đã tìm thấy và thêm môn: {names}!\n\n"
    low = user_text.lower().strip()
    if any(k in low for k in ['sao rồi', 'sao roi', 'tình hình', 'tinh hinh', 'thế nào', 'the nao']):
        return f"💬 Bạn hỏi: \"{user_text}\"\n👉 Báo cáo: Mọi thứ vẫn đang chạy tự động ngon lành! Dưới đây là tình hình các môn học ngay lúc này:\n\n"
    if any(k in low for k in ['làm chưa', 'lam chua', 'làm bài chưa', 'lam bai chua', 'làm bài', 'lam bai', 'học chưa', 'hoc chua', 'chạy chưa', 'chay chua', 'xong chưa', 'xong chua']):
        return f"💬 Bạn hỏi: \"{user_text}\"\n👉 Báo cáo: Đã kiểm tra và làm sạch 100% các bài đã mở cho bạn rồi nhé! Chi tiết từng môn:\n\n"
    return f"💬 Bạn nhắn: \"{user_text}\"\n👉 Bot phản hồi trạng thái học tập mới nhất trên eTUAF:\n\n"

def check_pending_zalo_questions(wait_timeout=25):
    """Poll Zalo Bot getUpdates for incoming user message."""
    if not ZALO_BOT_TOKEN or STOP_REQUESTED:
        return None, None
    base_url = f"https://bot-api.zaloplatforms.com/bot{ZALO_BOT_TOKEN}"
    reply_cid, last_text = None, None
    try:
        r = requests.post(f"{base_url}/getUpdates", json={"timeout": wait_timeout}, timeout=wait_timeout + 6)
        data = r.json()
        if data.get("ok") and data.get("result"):
            msg_obj = data["result"].get("message", {})
            cid = msg_obj.get("chat", {}).get("id")
            txt = msg_obj.get("text", "")
            if cid:
                reply_cid, last_text = str(cid), str(txt)
                log(f"Received Zalo message from {reply_cid}: {last_text}")
    except Exception:
        pass
    return reply_cid, last_text

def norm(s):
    return ' '.join(s.split()).strip() if s else ''

def norm_choice(s):
    s = norm(s)
    return re.sub(r'^[a-dA-D][\.\)]\s*', '', s).strip()

def extract_choice_text(ans_container):
    label_el = ans_container.find('div', class_='flex-fill')
    if label_el:
        return norm_choice(label_el.get_text())
    clone = BeautifulSoup(str(ans_container), 'html.parser')
    for bad in clone.select('.answernumber, .specificfeedback, script, style'):
        bad.decompose()
    return norm_choice(clone.get_text())

def make_q_key(q_div):
    qtext_el = q_div.find(class_='qtext')
    qtext = norm(qtext_el.get_text()) if qtext_el else ''
    imgs = [img.get('src', '').split('/')[-1] for img in q_div.select('.qtext img')] if qtext_el else []
    ans_divs = q_div.select('.answer div.r0, .answer div.r1')
    choices = sorted(extract_choice_text(a) for a in ans_divs)
    return f"{qtext} [{' '.join(imgs)}] || {' | '.join(choices)}"

def wait_for_network_and_login(max_retries=5, delay_sec=5):
    login_url = 'https://tuaf.aum.edu.vn/login/index.php'
    my_url = 'https://tuaf.aum.edu.vn/my/'
    for attempt in range(1, max_retries + 1):
        try:
            r_my = safe_get(my_url, timeout=20)
            if 'login/index.php' not in r_my.url:
                return r_my.text
            soup = BeautifulSoup(r_my.text, 'html.parser')
            token_inp = soup.find('input', {'name': 'logintoken'})
            logintoken = token_inp['value'] if token_inp else ''
            r2 = safe_post(login_url, data={
                'username': USERNAME,
                'password': PASSWORD,
                'logintoken': logintoken
            }, timeout=20)
            log(f"Logged in successfully: {r2.url}")
            if 'my' not in r2.url:
                r2 = safe_get(my_url, timeout=20)
            return r2.text
        except Exception as e:
            log(f"Network/login attempt {attempt}/{max_retries} failed ({e}), retrying in {delay_sec}s...")
            time.sleep(delay_sec)
    raise RuntimeError("Could not connect to tuaf.aum.edu.vn after retries.")

def discover_courses_from_dashboard(dashboard_html, user_text=None):
    soup = BeautifulSoup(dashboard_html, 'html.parser')
    seen = {}
    for a in soup.find_all('a', href=re.compile(r'/course/view\.php\?id=\d+')):
        m = re.search(r'id=(\d+)', a['href'])
        if not m:
            continue
        cid = int(m.group(1))
        name = norm(a.get_text())
        if not name or name.lower() in ('xem', 'view', 'vào học'):
            continue
        if cid not in seen or len(name) > len(seen[cid]):
            seen[cid] = name

    all_courses = [(cid, cname) for cid, cname in seen.items()]
    matched_new = []
    norm_user = strip_accents(user_text) if user_text else ''

    for cid, cname in all_courses:
        m_batch = re.search(r'-\s*(\d{6})\b', cname)
        if m_batch and int(m_batch.group(1)) >= MIN_AUTO_BATCH:
            if cid not in ACTIVE_COURSE_IDS:
                ACTIVE_COURSE_IDS.append(cid)
                log(f"Auto-discovered new batch course: {cid} ({cname})")

        if norm_user and len(norm_user) >= 4:
            base_name = re.sub(r'\s*-\s*\d{6}.*$', '', cname).strip()
            norm_course = strip_accents(base_name)
            if norm_course and (norm_course in norm_user or (len(norm_user) >= 6 and norm_user in norm_course)):
                if cid not in ACTIVE_COURSE_IDS:
                    ACTIVE_COURSE_IDS.append(cid)
                matched_new.append((cid, cname))
                log(f"Matched course from user Zalo text: {cid} ({cname})")

    return all_courses, matched_new

def complete_scorm(cmid, name=""):
    log(f"[SCORM] Completing {cmid} ({name})...")
    vurl = f'https://tuaf.aum.edu.vn/mod/scorm/view.php?id={cmid}'
    vresp = safe_get(vurl)
    vsoup = BeautifulSoup(vresp.text, 'html.parser')
    form = vsoup.find('form', {'id': 'scormviewform'})
    if not form:
        log(f"  SCORM {cmid} locked or no scormviewform.")
        return False
    scoid_inp = form.find('input', {'name': 'scoid'})
    scoid = scoid_inp['value'] if scoid_inp else ''
    player_url = f'https://tuaf.aum.edu.vn/mod/scorm/player.php?a={form.find("input", {"name": "a"})["value"]}&currentorg=&scoid={scoid}&sesskey={form.find("input", {"name": "sesskey"})["value"]}&display=popup&mode=normal'
    presp = safe_get(player_url)
    m_sess = re.search(r'"sesskey"\s*:\s*"([^"]+)"', presp.text)
    sesskey = m_sess.group(1) if m_sess else form.find("input", {"name": "sesskey"})["value"]
    a_id = form.find("input", {"name": "a"})["value"]

    datamodel_url = 'https://tuaf.aum.edu.vn/mod/scorm/datamodel.php'
    payload = {
        'id': str(cmid),
        'a': str(a_id),
        'scoid': str(scoid),
        'attempt': '1',
        'sesskey': sesskey,
        'cmi__core__lesson_status': 'completed',
        'cmi__core__score__raw': '100',
        'cmi__core__score__min': '0',
        'cmi__core__score__max': '100',
        'cmi__core__total_time': '00:25:13.00',
        'cmi__core__session_time': '00:25:13.00',
        'cmi__core__exit': ''
    }
    dresp = safe_post(datamodel_url, data=payload)
    log(f"  SCORM {cmid} datamodel response: {dresp.text.strip()}")
    return 'true' in dresp.text.lower()

def parse_review_page(html, known_correct, known_wrong):
    soup = BeautifulSoup(html, 'html.parser')
    total = 0
    correct_cnt = 0
    for q in soup.find_all('div', id=re.compile(r'^question-\d+-\d+$')):
        total += 1
        qkey = make_q_key(q)
        classes = q.get('class', [])
        is_correct = 'correct' in classes

        fb = q.find(class_='rightanswer')
        if fb:
            fb_txt = norm(fb.get_text())
            m = re.search(r'(?:Đáp án đúng là|The correct answer is)\s*:\s*(.+)$', fb_txt, re.I)
            if m:
                known_correct[qkey] = norm_choice(m.group(1))

        checked_inp = q.find('input', {'type': 'radio', 'checked': True})
        if not checked_inp:
            for inp in q.find_all('input', {'type': 'radio'}):
                if inp.has_attr('checked'):
                    checked_inp = inp
                    break

        if checked_inp:
            ans_div = checked_inp.find_parent('div', class_=re.compile(r'^r[01]$'))
            chosen_txt = extract_choice_text(ans_div) if ans_div else ''
            if is_correct:
                correct_cnt += 1
                if chosen_txt:
                    known_correct[qkey] = chosen_txt
            else:
                if chosen_txt:
                    known_wrong.setdefault(qkey, set()).add(chosen_txt)
    return correct_cnt, total

def run_quiz_attempt(cmid, known_correct, known_wrong):
    view_url = f'https://tuaf.aum.edu.vn/mod/quiz/view.php?id={cmid}'
    vresp = safe_get(view_url)
    vsoup = BeautifulSoup(vresp.text, 'html.parser')
    sesskey_inp = vsoup.find('input', {'name': 'sesskey'})
    if not sesskey_inp:
        m = re.search(r'"sesskey"\s*:\s*"([^"]+)"', vresp.text)
        if not m:
            log(f"  Quiz {cmid} cannot find sesskey (maybe locked).")
            return None
        sesskey = m.group(1)
    else:
        sesskey = sesskey_inp['value']

    start_resp = safe_post('https://tuaf.aum.edu.vn/mod/quiz/startattempt.php', data={
        'cmid': str(cmid),
        'sesskey': sesskey
    })
    m_att = re.search(r'attempt=(\d+)', start_resp.url)
    if not m_att:
        m_att = re.search(r'name="attempt"\s+value="(\d+)"', start_resp.text)
    if not m_att:
        log(f"  Quiz {cmid} could not start attempt. URL: {start_resp.url}")
        return None
    attempt_id = m_att.group(1)

    page = 0
    while True:
        att_url = f'https://tuaf.aum.edu.vn/mod/quiz/attempt.php?attempt={attempt_id}&cmid={cmid}'
        if page > 0:
            att_url += f'&page={page}'
        aresp = safe_get(att_url)
        asoup = BeautifulSoup(aresp.text, 'html.parser')
        form = asoup.find('form', {'id': 'responseform'})
        if not form:
            break

        post_data = {}
        for inp in form.find_all('input'):
            name = inp.get('name')
            itype = inp.get('type', '').lower()
            if not name or itype in ('radio', 'submit', 'button'):
                continue
            post_data[name] = inp.get('value', '')

        q_divs = form.find_all('div', id=re.compile(r'^question-\d+-\d+$'))
        for q in q_divs:
            qkey = make_q_key(q)
            ans_divs = q.select('.answer div.r0, .answer div.r1')
            chosen_val = None
            chosen_name = None

            options = []
            for adiv in ans_divs:
                r_inp = adiv.find('input', {'type': 'radio'})
                if not r_inp:
                    continue
                r_name = r_inp.get('name')
                r_val = r_inp.get('value')
                c_txt = extract_choice_text(adiv)
                options.append((r_name, r_val, c_txt))

            if not options:
                continue

            chosen_name = options[0][0]
            if qkey in known_correct:
                target = known_correct[qkey]
                for r_name, r_val, c_txt in options:
                    if c_txt == target:
                        chosen_val = r_val
                        break

            if chosen_val is None:
                wrong_set = known_wrong.get(qkey, set())
                untried = [(rn, rv, ct) for (rn, rv, ct) in options if ct not in wrong_set]
                if untried:
                    chosen_val = untried[0][1]
                else:
                    chosen_val = options[0][1]

            if chosen_name and chosen_val is not None:
                post_data[chosen_name] = chosen_val

        next_inp = form.find('input', {'name': 'nextpage'})
        nextpage_val = int(next_inp['value']) if (next_inp and next_inp.get('value', '').lstrip('-').isdigit()) else -1
        post_data['next'] = 'Next page'
        safe_post(f'https://tuaf.aum.edu.vn/mod/quiz/processattempt.php?cmid={cmid}', data=post_data)
        if nextpage_val == -1 or nextpage_val <= page:
            break
        page = nextpage_val

    fin_resp = safe_post('https://tuaf.aum.edu.vn/mod/quiz/processattempt.php', data={
        'attempt': attempt_id,
        'finishattempt': '1',
        'timeup': '0',
        'slots': '',
        'cmid': str(cmid),
        'sesskey': sesskey
    })
    return fin_resp, attempt_id

def preload_course_quiz_bank(csoup, known_correct, known_wrong):
    for act in csoup.find_all('li', class_=re.compile(r'activity')):
        if 'modtype_quiz' not in ' '.join(act.get('class', [])):
            continue
        a_tag = act.find('a', class_='aalink') or act.find('a')
        if not a_tag or not a_tag.get('href'):
            continue
        try:
            vresp = safe_get(a_tag['href'])
            vsoup = BeautifulSoup(vresp.text, 'html.parser')
            for rev_a in vsoup.find_all('a', href=re.compile(r'/mod/quiz/review\.php\?attempt=\d+')):
                parse_review_page(safe_get(rev_a['href']).text, known_correct, known_wrong)
        except Exception:
            pass

def complete_quiz(cmid, name="", known_correct=None, known_wrong=None):
    log(f"[QUIZ] Completing {cmid} ({name})...")
    if known_correct is None:
        known_correct = {}
    if known_wrong is None:
        known_wrong = {}
    vresp = safe_get(f'https://tuaf.aum.edu.vn/mod/quiz/view.php?id={cmid}')
    vsoup = BeautifulSoup(vresp.text, 'html.parser')
    for a in vsoup.find_all('a', href=re.compile(r'/mod/quiz/review\.php\?attempt=\d+')):
        parse_review_page(safe_get(a['href']).text, known_correct, known_wrong)

    best_c, best_t = 0, 0
    for attempt_num in range(1, 7):
        res = run_quiz_attempt(cmid, known_correct, known_wrong)
        if not res:
            break
        fin_resp, attempt_id = res
        c_cnt, t_cnt = parse_review_page(fin_resp.text, known_correct, known_wrong)
        if c_cnt >= best_c:
            best_c, best_t = c_cnt, t_cnt
        log(f"  Attempt #{attempt_num} (id={attempt_id}): {c_cnt}/{t_cnt} correct")
        if t_cnt > 0 and c_cnt == t_cnt:
            break
    return best_c, best_t

def scan_and_complete_courses(course_ids=None, reply_cid=None, user_text=None):
    with SCAN_LOCK:
        dashboard_html = wait_for_network_and_login()
        all_courses, matched_new = discover_courses_from_dashboard(dashboard_html, user_text=user_text)

        if user_text and any(k in strip_accents(user_text) for k in ['danh sach mon', 'cac mon', 'ds mon']):
            lines = [f"• {cname} {'(Đang tự động học)' if cid in ACTIVE_COURSE_IDS else ''}" for cid, cname in all_courses]
            send_zalo_notification("📚 Danh sách các môn trên tài khoản eTUAF của bạn:\n\n" + "\n".join(lines), chat_id=reply_cid)
            return

        target_ids = list(ACTIVE_COURSE_IDS) if course_ids is None else course_ids
        completed_summary = []
        course_status_lines = []

        for cid in target_ids:
            url = f'https://tuaf.aum.edu.vn/course/view.php?id={cid}'
            resp = safe_get(url)
            csoup = BeautifulSoup(resp.text, 'html.parser')
            course_title = csoup.title.string.replace('Khóa:', '').replace('| eTUAF', '').strip() if csoup.title else f'Course {cid}'
            log(f"Scanning COURSE {cid}: {course_title}")

            course_known_correct = {}
            course_known_wrong = {}
            tests_to_run = []
            done_count = 0
            locked_count = 0
            newly_done_in_course = 0

            for act in csoup.find_all('li', class_=re.compile(r'activity')):
                classes = ' '.join(act.get('class', []))
                comp_btn = act.find(attrs={'data-region': 'completion-info'})
                comp_state = norm(comp_btn.get_text()) if comp_btn else ''
                if not comp_state:
                    continue
                if 'Cần:' not in comp_state:
                    done_count += 1
                    continue

                a_tag = act.find('a', class_='aalink') or act.find('a')
                href = a_tag.get('href', '') if a_tag else ''
                m_id = re.search(r'id=(\d+)', href)
                if not m_id:
                    continue
                cmid = int(m_id.group(1))
                name_el = act.find(class_='instancename')
                name = norm(name_el.get_text()) if name_el else ''

                try:
                    vresp = safe_get(href)
                    vsoup = BeautifulSoup(vresp.text, 'html.parser')
                    if 'modtype_scorm' in classes:
                        if vsoup.find('form', {'id': 'scormviewform'}):
                            if complete_scorm(cmid, name):
                                completed_summary.append(f"✅ [{course_title}] {name}: Đã hoàn thành 100%")
                                done_count += 1
                                newly_done_in_course += 1
                        else:
                            locked_count += 1
                    elif 'modtype_quiz' in classes:
                        if vsoup.find('form', {'action': re.compile(r'startattempt\.php')}):
                            if 'Bài kiểm tra' in name:
                                tests_to_run.append((cmid, name))
                            else:
                                c_cnt, t_cnt = complete_quiz(cmid, name, course_known_correct, course_known_wrong)
                                if t_cnt > 0:
                                    completed_summary.append(f"✅ [{course_title}] {name}: Đạt {c_cnt}/{t_cnt} câu ({round(c_cnt*10/t_cnt, 2)}/10 điểm)")
                                    done_count += 1
                                    newly_done_in_course += 1
                        else:
                            locked_count += 1
                    else:
                        if 'Cần: Xem' in comp_state and vresp.status_code == 200:
                            completed_summary.append(f"✅ [{course_title}] {name}: Đã xem & hoàn thành")
                            done_count += 1
                            newly_done_in_course += 1
                        else:
                            locked_count += 1
                except Exception as act_err:
                    log(f"  Warning: failed checking activity {cmid}: {act_err}")

            if tests_to_run:
                preload_course_quiz_bank(csoup, course_known_correct, course_known_wrong)
                for cmid, name in tests_to_run:
                    try:
                        c_cnt, t_cnt = complete_quiz(cmid, name, course_known_correct, course_known_wrong)
                        if t_cnt > 0:
                            completed_summary.append(f"🏆 [{course_title}] {name}: Đạt {c_cnt}/{t_cnt} câu ({round(c_cnt*10/t_cnt, 2)}/10 điểm)")
                            done_count += 1
                            newly_done_in_course += 1
                    except Exception as q_err:
                        log(f"  Warning: failed quiz {cmid}: {q_err}")

            course_status_lines.append(
                f"📘 {course_title}:\n"
                f"   • Đã hoàn thành: {done_count} bài"
                + (f" (vừa làm mới {newly_done_in_course} bài)" if newly_done_in_course else "")
                + f"\n   • Chưa mở: {locked_count} bài"
            )

        now_str = datetime.now(VN_TZ).strftime('%H:%M ngày %d/%m/%Y')
        prefix = build_conversational_prefix(user_text, matched_new_courses=matched_new)
        if completed_summary:
            msg = (
                prefix
                + f"🎉 [eTUAF Auto - {now_str}]\n"
                + "Đã tự động hoàn thành các bài mới mở:\n\n"
                + "\n".join(completed_summary)
                + "\n\n📊 Tổng kết hiện tại:\n"
                + "\n".join(course_status_lines)
            )
            send_zalo_notification(msg, chat_id=reply_cid)
        elif reply_cid or '--notify' in sys.argv:
            msg = (
                prefix
                + "\n".join(course_status_lines)
                + f"\n\n✅ Tính đến {now_str}: Tất cả các Bài giảng điện tử, Bài luyện tập và Bài kiểm tra đã mở đều đã HOÀN THÀNH 100%!"
            )
            send_zalo_notification(msg, chat_id=reply_cid)

def cloud_listener_loop():
    """
    Dedicated 2-thread architecture:
    - Thread 1 (Main): Holds Zalo Bot getUpdates open 100% of the time with zero blind spots.
    - Thread 2 (Worker): Runs initial/periodic eTUAF scans and processes Zalo queries from WORK_QUEUE.
    """
    log("Starting dedicated 2-thread cloud listener...")
    work_queue = queue.Queue()

    def worker_loop():
        # Run initial scan in background without blocking Zalo poller
        try:
            scan_and_complete_courses()
        except Exception as e:
            log(f"Initial background scan warning: {e}")

        last_periodic = time.time()
        while not STOP_REQUESTED:
            try:
                cid, utxt = work_queue.get(timeout=5)
                try:
                    scan_and_complete_courses(reply_cid=cid, user_text=utxt)
                except Exception as e:
                    log(f"Error handling Zalo query: {e}")
                    send_zalo_notification(f"⚠️ Máy chủ eTUAF đang phản hồi chậm ({e}), đại ca thử lại sau ít phút nhé!", chat_id=cid)
                last_periodic = time.time()
            except queue.Empty:
                if (time.time() - last_periodic) > 1800:
                    try:
                        scan_and_complete_courses()
                    except Exception as e:
                        log(f"Periodic scan warning: {e}")
                    last_periodic = time.time()

    worker = threading.Thread(target=worker_loop, daemon=True)
    worker.start()

    if os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch':
        send_zalo_notification("🟢 Bot đám mây GitHub đã sẵn sàng trực tuyến! Đại ca có thể nhắn lệnh bất cứ lúc nào.")

    end_time = time.time() + 55 * 60
    try:
        while time.time() < end_time and not STOP_REQUESTED:
            cid, utxt = check_pending_zalo_questions(wait_timeout=25)
            if cid:
                send_zalo_notification(
                    f"⏳ Đã nhận tin nhắn \"{utxt}\"! Đang kiểm tra trực tiếp trên eTUAF, bạn đợi khoảng 8 giây nhé...",
                    chat_id=cid
                )
                work_queue.put((cid, utxt))
    except KeyboardInterrupt:
        log("Interrupted cleanly, exiting 0.")
        os._exit(0)

if __name__ == '__main__':
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        cloud_listener_loop()
    else:
        cids = [int(x) for x in sys.argv[1:] if x.isdigit()]
        scan_and_complete_courses(cids if cids else None)
