# GeoPulse Local Setup & Environment Guide

This document details the step-by-step instructions to configure the GeoPulse development environment on Windows and cross-platform systems.

---

## 1. Prerequisites Audit

Based on the Day 1 system audit:
- **Operating System**: Windows (x64)
- **Python**: 3.12.10 (`C:\Users\HP\AppData\Local\Programs\Python\Python312\python.exe`)
- **Git**: 2.55.0+
- **Node.js**: v24.20.0 (npm 11.19.0)
- **Java**: OpenJDK 21 LTS (Temurin)

---

## 2. Python Environment Setup

Because `python` is not currently in the Windows `PATH` environment variable, use the explicit binary path or configure a local virtual environment:

### Step 2.1: Create a Virtual Environment (.venv)
In the project root directory (`D:\Projects\GeoPulse`), run:
```powershell
& "C:\Users\HP\AppData\Local\Programs\Python\Python312\python.exe" -m venv .venv
```

### Step 2.2: Activate the Virtual Environment
```powershell
.\.venv\Scripts\Activate.ps1
```
*(If PowerShell displays an ExecutionPolicy error, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned` in your current terminal session).*

### Step 2.3: Upgrade pip and Install Day 1 Dependencies
```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 3. Staged Dependency Installation Plan

To maintain stability and avoid installation bloat on Day 1, install packages in dedicated phases:

### Phase 1: Foundation (Day 1 - Current)
- **Scope**: Configuration parsing, Pydantic data schemas, testing frameworks, code formatting.
- **Commands**:
  ```powershell
  pip install -r requirements.txt
  pip install -r requirements-dev.txt
  ```

### Phase 2: Spatial & Distributed Computing (Day 4)
- **Scope**: Shapely, GeoPandas, PySpark, Apache Sedona, H3 spatial indexing.
- **Commands**:
  ```powershell
  pip install -r requirements-spatial.txt
  ```
- **Windows Hadoop Configuration**:
  On Windows, Apache Spark requires `winutils.exe` and `hadoop.dll` for local file I/O:
  1. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from the trusted repo: `https://github.com/cdarlint/winutils`
  2. Place them in `C:\hadoop\bin\`
  3. Set environment variable: `[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")`
- **Java 21 with PySpark Note**:
  PySpark 3.5 requires the following JVM options when running on Java 21:
  ```
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  ```
  *Tip*: Installing Eclipse Temurin JDK 17 LTS is an alternative that simplifies PySpark native compatibility.

### Phase 3: Warehouse & Transformations (Day 6)
- **Scope**: Snowflake connector, Snowpark, dbt-snowflake.
- **Commands**:
  ```powershell
  pip install -r requirements-snowflake.txt
  ```

### Phase 4: Frontend Dashboard (Day 3 / Day 9)
- **Scope**: React 18, Kepler.gl, Vite, Deck.gl.
- **Commands**:
  ```powershell
  cd src/member2_analytics_viz/dashboard
  cmd.exe /c npm install
  cmd.exe /c npm run dev
  ```

---

## 4. Verification Commands for Day 1
To verify your project structure and configuration without altering git or installing heavy packages:

```powershell
# 1. Check directory tree
tree /F /A

# 2. Verify base configuration is valid YAML
& "C:\Users\HP\AppData\Local\Programs\Python\Python312\python.exe" -c "import yaml; print(yaml.safe_load(open('config/base.yaml'))['project']['name'])"

# 3. Check git status (ensure no unauthorized git init was performed)
git status
```
