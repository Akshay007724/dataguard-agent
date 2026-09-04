import os
import math
from PIL import Image, ImageDraw, ImageFont

WIDTH = 1100
HEIGHT = 710

def get_font(size, bold=False):
    font_paths = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Menlo.ttc"
    ]
    for p in font_paths:
        if os.path.exists(p):
            try:
                idx = 1 if (bold and p.endswith(".ttc")) else 0
                return ImageFont.truetype(p, size, index=idx)
            except Exception:
                try:
                    return ImageFont.truetype(p, size)
                except Exception:
                    pass
    return ImageFont.load_default()

font_title = get_font(20, bold=True)
font_title_sub = get_font(12, bold=False)
font_pill = get_font(11, bold=True)
font_node_title = get_font(13, bold=True)
font_node_sub = get_font(11, bold=False)
font_node_badge = get_font(10, bold=True)
font_term_head = get_font(11, bold=True)
font_term_code = get_font(12, bold=False)
font_term_code_bold = get_font(12, bold=True)

# Layout: Clean geometric spacing
NODES = {
    "agent": {
        "x": 40, "y": 95, "w": 285, "h": 105,
        "title": "LLM Client / Agent",
        "sub1": "Claude Desktop • Cursor • TriageAgent",
        "sub2": "Protocol: FastMCP JSON-RPC / SSE",
        "badge": "MCP CLIENT",
        "color": (56, 189, 248),      # Cyan
    },
    "redis": {
        "x": 410, "y": 95, "w": 280, "h": 105,
        "title": "Redis 7 Mutex & Cache",
        "sub1": "Distributed Lock (TTL=60s)",
        "sub2": "UUID Token Mutex • Atomic Lua",
        "badge": "DISTRIBUTED LOCK",
        "color": (244, 63, 94),       # Neon Coral/Red
    },
    "orchestrator": {
        "x": 775, "y": 95, "w": 285, "h": 105,
        "title": "Orchestrator REST API",
        "sub1": "Airflow 2.x • Argo Workflows",
        "sub2": "Async HTTP Log & State Extractor",
        "badge": "ASYNC ADAPTER",
        "color": (245, 158, 11),      # Amber/Gold
    },
    "matcher": {
        "x": 40, "y": 250, "w": 285, "h": 105,
        "title": "Regex Fast-Path Matcher",
        "sub1": "Deterministic Signature Scanner",
        "sub2": "OOM, Timeout, Schema (<10ms)",
        "badge": "FAST-PATH BYPASS",
        "color": (16, 185, 129),      # Neon Emerald
    },
    "sentinel": {
        "x": 375, "y": 235, "w": 350, "h": 130,
        "title": "Pipeline Sentinel Engine",
        "sub1": "Central MCP Server & FastAPI (:8080)",
        "sub2": "Concurrency Coordinator & Tool Registry",
        "badge": "CORE SENTINEL HUB",
        "color": (168, 85, 247),      # Vivid Violet
    },
    "detectors": {
        "x": 775, "y": 250, "w": 285, "h": 105,
        "title": "Lineage & Quality Detectors",
        "sub1": "Marquez OpenLineage Graph Trace",
        "sub2": "Schema Drift, Freshness & Volume",
        "badge": "CONCURRENT RAG",
        "color": (99, 102, 241),      # Indigo
    },
    "postgres": {
        "x": 160, "y": 405, "w": 350, "h": 105,
        "title": "PostgreSQL 16 Repositories",
        "sub1": "Incidents • Remediation Plans • Audits",
        "sub2": "SQLAlchemy 2.0 Async System of Record",
        "badge": "ASYNC REPOSITORIES",
        "color": (2, 132, 199),       # Sky Blue
    },
    "llm": {
        "x": 590, "y": 405, "w": 350, "h": 105,
        "title": "LiteLLM Multi-Model Engine",
        "sub1": "Claude 3.5 • GPT-4o • Ollama • Groq",
        "sub2": "Structured Pydantic DiagnosisResult",
        "badge": "STRUCTURED REASONING",
        "color": (236, 72, 153),      # Fuchsia Pink
    },
}

def get_node_center(key):
    n = NODES[key]
    return (n["x"] + n["w"] // 2, n["y"] + n["h"] // 2)

STEPS = [
    {
        "step_num": 1,
        "name": "STEP 1/9: AUTONOMOUS TRIAGE INGESTION",
        "src": "agent", "dst": "sentinel",
        "active_nodes": ["agent", "sentinel"],
        "color": (56, 189, 248),
        "tag": "INGEST",
        "term_cmd": "diagnose_failure(pipeline_id='daily_revenue_etl', run_id='run_2026_9082')",
        "term_out": ">> Protocol: FastMCP JSON-RPC | Client: Claude Desktop | Status: 202 Accepted",
        "detail": "LLM client detects pipeline failure webhook and invokes Sentinel triage agent",
    },
    {
        "step_num": 2,
        "name": "STEP 2/9: DISTRIBUTED MUTEX LOCKING",
        "src": "sentinel", "dst": "redis",
        "active_nodes": ["sentinel", "redis"],
        "color": (244, 63, 94),
        "tag": "MUTEX",
        "term_cmd": "Redis.set('lock:diagnose:daily_revenue_etl', token='uuid4:e8f2...', nx=True, ex=60)",
        "term_out": ">> Distributed Lock ACQUIRED (TTL 60s) | Concurrency race condition prevented",
        "detail": "Eliminates duplicate concurrent triage runs across horizontal agent instances",
    },
    {
        "step_num": 3,
        "name": "STEP 3/9: RETRIEVE FAILURE DETAILS",
        "src": "sentinel", "dst": "orchestrator",
        "active_nodes": ["sentinel", "orchestrator"],
        "color": (245, 158, 11),
        "tag": "ADAPTER",
        "term_cmd": "AirflowAdapter.get_failure_details('daily_revenue_etl', 'run_2026_9082')",
        "term_out": ">> HTTP 200 OK | Failing Task: 'transform_orders' | Fetched head/tail truncated logs",
        "detail": "Asynchronously queries orchestrator REST API for task exit code, traceback & state",
    },
    {
        "step_num": 4,
        "name": "STEP 4/9: DETERMINISTIC FAST-PATH SCAN",
        "src": "sentinel", "dst": "matcher",
        "active_nodes": ["sentinel", "matcher"],
        "color": (16, 185, 129),
        "tag": "FASTPATH",
        "term_cmd": "RegexMatcher.scan(logs, patterns=['OOM', 'Timeout', 'KeyError', 'SchemaDrift'])",
        "term_out": ">> Scan: 2.8ms | Result: Novel Failure (Branching to concurrent multi-source RAG)",
        "detail": "High-frequency infrastructure errors resolve instantly (<10ms) saving 100% LLM tokens",
    },
    {
        "step_num": 5,
        "name": "STEP 5/9: CONCURRENT EVIDENCE GATHERING",
        "src": "sentinel", "dst": "detectors",
        "active_nodes": ["sentinel", "detectors"],
        "color": (99, 102, 241),
        "tag": "LINEAGE",
        "term_cmd": "asyncio.gather(Marquez.trace_upstream(), DriftDetector.check(), IncidentRepo.find())",
        "term_out": ">> Lineage: 3 upstream nodes | Drift: column 'tax_amt' altered | Similar: INC-042",
        "detail": "Pulls upstream dependency graph, schema assertions, and past resolutions in parallel",
    },
    {
        "step_num": 6,
        "name": "STEP 6/9: STRUCTURED LLM REASONING",
        "src": "sentinel", "dst": "llm",
        "active_nodes": ["sentinel", "llm"],
        "color": (236, 72, 153),
        "tag": "REASONING",
        "term_cmd": "LiteLLM.complete_structured(prompt=context, schema=DiagnosisResult)",
        "term_out": ">> Model: Claude 3.5 Sonnet | Category: schema_drift | Confidence: 0.96 | Tokens: 412",
        "detail": "Synthesizes diagnostic evidence into strict Pydantic model with root cause & fix",
    },
    {
        "step_num": 7,
        "name": "STEP 7/9: SYSTEM-OF-RECORD PERSISTENCE",
        "src": "sentinel", "dst": "postgres",
        "active_nodes": ["sentinel", "postgres"],
        "color": (2, 132, 199),
        "tag": "DATABASE",
        "term_cmd": "IncidentRepository.create(incident_id='INC-089', severity='P2', status='OPEN')",
        "term_out": ">> Committed to PostgreSQL 16 | RemediationPlan: 'alter table tax_amt to numeric(12,2)'",
        "detail": "Creates durable incident record, remediation steps, and immutable audit trail",
    },
    {
        "step_num": 8,
        "name": "STEP 8/9: ATOMIC MUTEX RELEASE",
        "src": "sentinel", "dst": "redis",
        "active_nodes": ["sentinel", "redis"],
        "color": (244, 63, 94),
        "tag": "UNLOCK",
        "term_cmd": "Redis.eval(LUA_RELEASE_SCRIPT, keys=['lock:diagnose...'], argv=['uuid4:e8f2...'])",
        "term_out": ">> Token verified via atomic Lua script | Mutex released cleanly (Lock freed)",
        "detail": "Safe lock termination ensures expired or renewed locks are never prematurely deleted",
    },
    {
        "step_num": 9,
        "name": "STEP 9/9: VERIFIED TRIAGE COMPLETE",
        "src": "sentinel", "dst": "agent",
        "active_nodes": ["sentinel", "agent"],
        "color": (16, 185, 129),
        "tag": "COMPLETE",
        "term_cmd": "FastMCP.respond(INC-089, status='DIAGNOSED', recommended_action='rerun_partition')",
        "term_out": ">> Triage lifecycle completed in 462ms | Ready for human-approved auto-remediation",
        "detail": "Agent client receives fully actionable root-cause report and safe remediation plan",
    },
]

def render_frame(step_idx, subframe_idx, total_subframes):
    step = STEPS[step_idx]
    progress_ratio = (step_idx + (subframe_idx / total_subframes)) / len(STEPS)
    
    # Deep midnight navy canvas
    img = Image.new("RGBA", (WIDTH, HEIGHT), (9, 13, 24, 255))
    draw = ImageDraw.Draw(img)
    
    # Grid pattern (every 24px)
    for gx in range(16, WIDTH, 24):
        for gy in range(16, HEIGHT, 24):
            draw.point((gx, gy), fill=(18, 28, 46, 255))
            
    # Top Header Banner
    draw.rectangle([(0, 0), (WIDTH, 72)], fill=(12, 17, 33, 255))
    draw.line([(0, 72), (WIDTH, 72)], fill=(28, 38, 56, 255), width=1)
    
    # Title & Subtitle
    draw.text((25, 15), "DATAGUARD AGENT", fill=(255, 255, 255, 255), font=font_title)
    draw.text((245, 16), "— PIPELINE SENTINEL", fill=(192, 132, 252, 255), font=font_title)
    draw.text((25, 43), "Autonomous Pipeline Observability & Self-Healing Execution Flow", fill=(148, 163, 184, 255), font=font_title_sub)
    
    # Live Status Badges (Top Right)
    pulse = math.sin((subframe_idx / total_subframes) * math.pi)
    pulse_alpha = int(170 + 85 * pulse)
    draw.ellipse([(690, 24), (704, 38)], fill=(16, 185, 129, pulse_alpha))
    draw.ellipse([(693, 27), (701, 35)], fill=(255, 255, 255, 255))
    draw.text((712, 23), "LIVE ON FLY.IO (:8080)", fill=(52, 211, 153, 255), font=font_pill)
    
    # Step indicator pill (nicely centered)
    step_pill_text = step["name"].split(":")[0]
    draw.rounded_rectangle([(905, 18), (1075, 44)], radius=13, fill=(22, 30, 48, 255), outline=step["color"], width=1)
    sbox = font_pill.getbbox(step_pill_text)
    stext_w = sbox[2] - sbox[0]
    stext_x = 905 + (170 - stext_w) // 2
    draw.text((stext_x, 24), step_pill_text, fill=step["color"], font=font_pill)
    
    # Top progress bar
    bar_width = int(WIDTH * progress_ratio)
    draw.line([(0, 71), (bar_width, 71)], fill=step["color"], width=3)
    
    # Idle connection lines
    hub_center = get_node_center("sentinel")
    for k in NODES:
        if k == "sentinel":
            continue
        c = get_node_center(k)
        draw.line([hub_center, c], fill=(24, 36, 56, 255), width=2)
        
    # Active glowing beam between source and destination
    src_center = get_node_center(step["src"])
    dst_center = get_node_center(step["dst"])
    bc = step["color"]
    
    # Multi-layer neon beam
    draw.line([src_center, dst_center], fill=(bc[0], bc[1], bc[2], 60), width=8)
    draw.line([src_center, dst_center], fill=(bc[0], bc[1], bc[2], 180), width=4)
    draw.line([src_center, dst_center], fill=(255, 255, 255, 240), width=2)
    
    # Traveling particle stream
    t = subframe_idx / float(total_subframes)
    px = int(src_center[0] + (dst_center[0] - src_center[0]) * t)
    py = int(src_center[1] + (dst_center[1] - src_center[1]) * t)
    
    # Photon tail
    for tail_i in range(1, 4):
        tail_t = max(0.0, t - tail_i * 0.09)
        tx = int(src_center[0] + (dst_center[0] - src_center[0]) * tail_t)
        ty = int(src_center[1] + (dst_center[1] - src_center[1]) * tail_t)
        tr = max(2, 6 - tail_i)
        draw.ellipse([(tx - tr, ty - tr), (tx + tr, ty + tr)], fill=(bc[0], bc[1], bc[2], 130 // tail_i))
        
    # Photon head
    draw.ellipse([(px - 11, py - 11), (px + 11, py + 11)], fill=(bc[0], bc[1], bc[2], 80))
    draw.ellipse([(px - 7, py - 7), (px + 7, py + 7)], fill=bc)
    draw.ellipse([(px - 3, py - 3), (px + 3, py + 3)], fill=(255, 255, 255, 255))
    
    # Destination arrival ripple
    if t >= 0.7:
        ripple_r = int((t - 0.7) * 45)
        ripple_alpha = int((1.0 - (t - 0.7) / 0.3) * 180)
        draw.ellipse([(dst_center[0] - ripple_r, dst_center[1] - ripple_r),
                      (dst_center[0] + ripple_r, dst_center[1] + ripple_r)],
                     outline=(bc[0], bc[1], bc[2], ripple_alpha), width=2)
        
    # Draw Nodes
    for k, n in NODES.items():
        is_active = k in step["active_nodes"]
        is_dest = (k == step["dst"]) and (subframe_idx >= total_subframes // 2)
        is_src = (k == step["src"]) and (subframe_idx < total_subframes // 2)
        is_focused = is_dest or is_src
        
        nx, ny, nw, nh = n["x"], n["y"], n["w"], n["h"]
        c = n["color"]
        
        if is_active:
            halo_pad = 5 if is_focused else 2
            draw.rounded_rectangle([(nx - halo_pad, ny - halo_pad), (nx + nw + halo_pad, ny + nh + halo_pad)],
                                  radius=12, fill=None, outline=(c[0], c[1], c[2], 100 if is_focused else 50), width=2)
            fill_bg = (int(c[0] * 0.16 + 14), int(c[1] * 0.16 + 20), int(c[2] * 0.16 + 32), 255)
            outline_col = (c[0], c[1], c[2], 255)
            border_w = 2
        else:
            fill_bg = (14, 20, 34, 255)
            outline_col = (30, 42, 62, 255)
            border_w = 1
            
        draw.rounded_rectangle([(nx, ny), (nx + nw, ny + nh)], radius=10, fill=fill_bg, outline=outline_col, width=border_w)
        
        # Left colored accent bar
        accent_alpha = 255 if is_active else 120
        draw.rounded_rectangle([(nx + 3, ny + 10), (nx + 7, ny + nh - 10)], radius=2, fill=(c[0], c[1], c[2], accent_alpha))
        
        # Node Title
        title_col = (255, 255, 255, 255) if is_active else (226, 232, 240, 255)
        draw.text((nx + 16, ny + 12), n["title"], fill=title_col, font=font_node_title)
        
        # Node Subtitles
        draw.text((nx + 16, ny + 38), n["sub1"], fill=(148, 163, 184, 255), font=font_node_sub)
        draw.text((nx + 16, ny + 56), n["sub2"], fill=(100, 116, 139, 255), font=font_node_sub)
        
        # Bottom row: Vector Status Circle on left, Badge on right
        dot_cy = ny + nh - 15
        if is_active:
            dot_color = c
            status_text = "EXECUTING" if is_focused else "CONNECTED"
            draw.ellipse([(nx + 16, dot_cy - 4), (nx + 24, dot_cy + 4)], fill=dot_color)
            draw.text((nx + 28, ny + nh - 21), status_text, fill=c, font=font_node_badge)
        else:
            draw.ellipse([(nx + 16, dot_cy - 3), (nx + 22, dot_cy + 3)], fill=(71, 85, 105, 255))
            draw.text((nx + 26, ny + nh - 21), "IDLE", fill=(100, 116, 139, 255), font=font_node_badge)
            
        # Right category badge in subtle pill container
        bbox = font_node_badge.getbbox(n["badge"])
        badge_text_w = bbox[2] - bbox[0]
        badge_w = badge_text_w + 14
        bx2 = nx + nw - 12
        bx1 = bx2 - badge_w
        by1 = ny + nh - 24
        by2 = by1 + 17
        draw.rounded_rectangle([(bx1, by1), (bx2, by2)], radius=5, fill=(20, 28, 44, 255), outline=(c[0], c[1], c[2], 160 if is_active else 60), width=1)
        draw.text((bx1 + 7, by1 + 2), n["badge"], fill=c if is_active else (148, 163, 184, 255), font=font_node_badge)

    # Bottom Terminal Console Box
    term_x = 35
    term_y = 530
    term_w = WIDTH - 70
    term_h = 160
    
    # Terminal frame
    draw.rounded_rectangle([(term_x, term_y), (term_x + term_w, term_y + term_h)], radius=10, fill=(7, 10, 18, 255), outline=(30, 41, 59, 255), width=1)
    
    # Terminal Header Bar
    draw.rounded_rectangle([(term_x, term_y), (term_x + term_w, term_y + 30)], radius=10, fill=(15, 23, 42, 255))
    draw.rectangle([(term_x, term_y + 20), (term_x + term_w, term_y + 30)], fill=(15, 23, 42, 255))
    draw.line([(term_x, term_y + 30), (term_x + term_w, term_y + 30)], fill=(30, 41, 59, 255), width=1)
    
    # macOS window action dots
    draw.ellipse([(term_x + 14, term_y + 10), (term_x + 24, term_y + 20)], fill=(239, 68, 68, 255))
    draw.ellipse([(term_x + 30, term_y + 10), (term_x + 40, term_y + 20)], fill=(245, 158, 11, 255))
    draw.ellipse([(term_x + 46, term_y + 10), (term_x + 56, term_y + 20)], fill=(16, 185, 129, 255))
    
    draw.text((term_x + 72, term_y + 8), "DATAGUARD SENTINEL REAL-TIME AUDIT & TRACE LOG", fill=(148, 163, 184, 255), font=font_term_head)
    
    # Step tag pill in terminal header
    t_tag = f"[{step['tag']}]"
    t_bbox = font_pill.getbbox(t_tag)
    stag_w = (t_bbox[2] - t_bbox[0]) + 16
    draw.rounded_rectangle([(term_x + term_w - stag_w - 15, term_y + 6), (term_x + term_w - 15, term_y + 24)],
                          radius=5, fill=(24, 34, 52, 255), outline=step["color"], width=1)
    draw.text((term_x + term_w - stag_w - 7, term_y + 8), t_tag, fill=step["color"], font=font_pill)
    
    # Terminal Log Contents
    draw.text((term_x + 18, term_y + 40), step["name"], fill=step["color"], font=font_term_head)
    
    prompt_str = "sentinel@dataguard:~$ "
    draw.text((term_x + 18, term_y + 64), prompt_str, fill=(52, 211, 153, 255), font=font_term_code_bold)
    p_box = font_term_code_bold.getbbox(prompt_str)
    cmd_x = term_x + 18 + (p_box[2] - p_box[0])
    draw.text((cmd_x, term_y + 64), step["term_cmd"], fill=(248, 250, 252, 255), font=font_term_code)
    
    # Cursor
    if subframe_idx % 2 == 0:
        c_box = font_term_code.getbbox(step["term_cmd"])
        cur_x = cmd_x + (c_box[2] - c_box[0]) + 3
        draw.rectangle([(cur_x, term_y + 64), (cur_x + 8, term_y + 78)], fill=(56, 189, 248, 255))
        
    draw.text((term_x + 18, term_y + 90), step["term_out"], fill=(226, 232, 240, 255), font=font_term_code)
    draw.text((term_x + 18, term_y + 116), f"AUDIT IMPACT: {step['detail']}", fill=(148, 163, 184, 255), font=font_node_sub)
    draw.text((term_x + 18, term_y + 134), f"ARCHITECTURE: FastMCP Router -> Distributed State Engine -> PostgreSQL System of Record", fill=(100, 116, 139, 255), font=font_node_sub)

    return img.convert("RGB")

def main():
    print("Generating refined animated GIF frames...")
    frames = []
    subframes_per_step = 5
    
    for s_idx in range(len(STEPS)):
        print(f"Rendering Step {s_idx + 1}/{len(STEPS)}: {STEPS[s_idx]['name']}...")
        for sub_idx in range(subframes_per_step):
            frame = render_frame(s_idx, sub_idx, subframes_per_step)
            q_frame = frame.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
            frames.append(q_frame)
            
    output_path = "docs/assets/live-action-diagram.gif"
    os.makedirs("docs/assets", exist_ok=True)
    
    print(f"Compiling {len(frames)} frames into {output_path}...")
    frames[0].save(
        output_path,
        save_all=True,
        append_images=frames[1:],
        duration=130,
        loop=0,
        optimize=True
    )
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"GIF successfully created at {output_path} ({file_size_kb:.1f} KB, {len(frames)} frames, infinite loop)")

if __name__ == "__main__":
    main()
