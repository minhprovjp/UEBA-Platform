# UBA Platform - Project File Structure

## Overview
This is a **User Behavior Analytics (UBA) Platform** designed to monitor and analyze database logs (MySQL, PostgreSQL, MongoDB) to detect anomalous user behaviors. The system consists of three main components: Analysis Engine, Backend API, and Frontend.

## Complete File Structure

```
UBA-Platform/
├── 📁 __pycache__/                    # Python cache files
├── 📁 backend_api/                    # FastAPI Backend Server
│   ├── 📄 __init__.py
│   ├── 📄 main_api.py                 # Main FastAPI application with endpoints
│   ├── 📄 models.py                   # SQLAlchemy database models
│   ├── 📄 schemas.py                  # Pydantic schemas for API validation
│   └── 📁 trained_models/
│       └── 📁 user_models/            # User-specific ML models
├── 📁 data/                          # Data storage directory
│   ├── 📄 app_database.db            # SQLite database
│   └── 📄 feedback.csv               # User feedback data
├── 📁 engine/                        # Analysis Engine (Core ML/AI Logic)
│   ├── 📄 __init__.py
│   ├── 📄 config_manager.py          # Configuration management
│   ├── 📄 data_processor.py          # Data processing and analysis
│   ├── 📄 email_alert.py             # Email notification system
│   ├── 📄 engine_runner.py           # Main engine orchestrator
│   ├── 📄 llm_analyzer.py            # LLM-based analysis
│   ├── 📄 llm_analyzer_dual.py       # Dual LLM analysis system
│   ├── 📄 mongodb_log_parser.py      # MongoDB log parser
│   ├── 📄 mysql_log_parser.py        # MySQL log parser
│   ├── 📄 postgres_log_parser.py     # PostgreSQL log parser
│   ├── 📄 temp.py                    # Temporary utilities
│   ├── 📄 utils.py                   # Utility functions
│   └── 📁 trained_models/            # ML models storage
│       ├── 📄 global_isolation_forest.joblib
│       └── 📁 user_models/           # User-specific models
│           ├── 📄 app.joblib
│           ├── 📄 dev.joblib
│           ├── 📄 limited_user.joblib
│           ├── 📄 postgres.joblib
│           ├── 📄 root.joblib
│           └── 📄 user1.joblib
├── 📁 logs/                          # Log files and parsed data
│   ├── 📁 mismatch/                  # Mismatched log files
│   ├── 📄 parsed_mysql_logs.csv      # Parsed MySQL logs
│   ├── 📄 parsed_mysql_logs.csv.meta # MySQL logs metadata
│   └── 📄 parsed_postgres_logs.csv  # Parsed PostgreSQL logs
├── 📁 trained_models/                # Global ML models (duplicate of engine/trained_models)
│   ├── 📄 global_isolation_forest.joblib
│   └── 📁 user_models/               # User-specific models
│       ├── 📄 app.joblib
│       ├── 📄 dev.joblib
│       ├── 📄 limited_user.joblib
│       ├── 📄 postgres.joblib
│       ├── 📄 root.joblib
│       └── 📄 user1.joblib
├── 📁 uba-frontend/                  # React TypeScript Frontend
│   ├── 📄 eslint.config.js           # ESLint configuration
│   ├── 📄 index.html                 # Main HTML file
│   ├── 📄 package.json               # Node.js dependencies
│   ├── 📄 package-lock.json          # Locked dependency versions
│   ├── 📄 README.md                  # Frontend documentation
│   ├── 📄 vite.config.ts             # Vite build configuration
│   ├── 📁 node_modules/              # Node.js dependencies
│   ├── 📁 public/                    # Static assets
│   │   └── 📄 vite.svg               # Vite logo
│   └── 📁 src/                       # Source code
│       ├── 📄 App.tsx                # Main React component
│       ├── 📄 App.css                # Main styles
│       ├── 📄 main.tsx               # React entry point
│       ├── 📄 index.css              # Global styles
│       ├── 📄 vite-env.d.ts          # Vite type definitions
│       ├── 📁 components/            # React components
│       │   ├── 📁 AnomalyExplorer/   # Anomaly exploration components
│       │   │   ├── 📄 AnomalyDetail.tsx
│       │   │   ├── 📄 AnomalyExplorer.css
│       │   │   └── 📄 AnomalyExplorerPage.tsx
│       │   ├── 📁 Configuration/     # Configuration components
│       │   │   ├── 📄 Configuration.css
│       │   │   └── 📄 ConfigurationPage.tsx
│       │   ├── 📁 Dashboard/         # Dashboard components
│       │   │   ├── 📄 AnomalyDetailModal.tsx
│       │   │   ├── 📄 AnomalyTable.tsx
│       │   │   ├── 📄 Charts.tsx
│       │   │   ├── 📄 Dashboard.css
│       │   │   ├── 📄 DashboardPage.tsx
│       │   │   └── 📄 StatCards.tsx
│       │   ├── 📁 EngineControl/     # Engine control components
│       │   │   ├── 📄 EngineControl.css
│       │   │   └── 📄 EngineControlPage.tsx
│       │   ├── 📁 Layout/            # Layout components
│       │   │   ├── 📄 Layout.css
│       │   │   ├── 📄 MainContent.tsx
│       │   │   └── 📄 Sidebar.tsx
│       │   └── 📁 UI/                # Reusable UI components
│       │       ├── 📄 Button.css
│       │       ├── 📄 Button.tsx
│       │       ├── 📄 Card.css
│       │       ├── 📄 Card.tsx
│       │       ├── 📄 index.ts
│       │       ├── 📄 LoadingSpinner.css
│       │       ├── 📄 LoadingSpinner.tsx
│       │       ├── 📄 SidebarToggle.css
│       │       ├── 📄 SidebarToggle.tsx
│       │       └── 📄 SidebarToggleDemo.tsx
│       ├── 📁 contexts/              # React contexts (empty)
│       ├── 📁 interfaces/            # TypeScript interfaces
│       │   └── 📄 Anomaly.ts         # Anomaly data interface
│       └── 📁 utils/                 # Utility functions (empty)
│   └── 📁 uba-frontend/              # Duplicate frontend directory (nested)
│       ├── 📄 eslint.config.js
│       ├── 📄 index.html
│       ├── 📄 package.json
│       ├── 📄 public/
│       │   └── 📄 vite.svg
│       ├── 📄 README.md
│       ├── 📁 src/
│       │   ├── 📄 App.css
│       │   ├── 📄 App.tsx
│       │   ├── 📄 assets/
│       │   │   └── 📄 react.svg
│       │   ├── 📄 index.css
│       │   ├── 📄 main.tsx
│       │   └── 📄 vite-env.d.ts
│       ├── 📄 tsconfig.app.json
│       ├── 📄 tsconfig.json
│       └── 📄 tsconfig.node.json
├── 📄 config.py                      # Main configuration file (generated from template)
├── 📄 config.py.template             # Configuration template
├── 📄 engine_config.json             # Engine configuration (generated from template)
├── 📄 engine_config.json.template    # Engine configuration template
├── 📄 requirements.txt               # Python dependencies
├── 📄 README.md                      # Project documentation
├── 📄 DUAL_LLM_SETUP.md             # Dual LLM setup guide
├── 📄 OLLAMA_SETUP.md               # Ollama setup guide
└── 📄 test_ollama_connection.py     # Ollama connection test script
```

## Project Architecture

### 🔧 **Analysis Engine** (`/engine/`)
The core component that runs continuously in the background to analyze database logs and detect anomalies.

**Key Files:**
- `engine_runner.py` - Main orchestrator that manages the analysis cycle
- `data_processor.py` - Processes parsed log data and applies detection rules
- `llm_analyzer.py` - LLM-based analysis for complex anomaly detection
- `llm_analyzer_dual.py` - Dual LLM system for enhanced analysis
- `*_log_parser.py` - Database-specific log parsers (MySQL, PostgreSQL, MongoDB)
- `config_manager.py` - Manages configuration settings
- `email_alert.py` - Sends email notifications for detected anomalies

**Features:**
- Real-time log monitoring
- Rule-based anomaly detection
- Machine learning models (Isolation Forest)
- LLM-powered analysis
- Multi-database support

### 🚀 **Backend API** (`/backend_api/`)
FastAPI-based RESTful API server that provides data access and engine control.

**Key Files:**
- `main_api.py` - Main FastAPI application with all endpoints
- `models.py` - SQLAlchemy database models
- `schemas.py` - Pydantic schemas for API validation

**Endpoints:**
- Anomaly data retrieval
- Engine control (start/stop/status)
- Configuration management
- Feedback submission

### 🎨 **Frontend** (`/uba-frontend/`)
Modern React TypeScript application providing a user-friendly interface.

**Key Components:**
- `Dashboard/` - Main dashboard with statistics and charts
- `AnomalyExplorer/` - Detailed anomaly exploration and analysis
- `EngineControl/` - Engine management interface
- `Configuration/` - System configuration management
- `Layout/` - Common layout components
- `UI/` - Reusable UI components

**Technology Stack:**
- React 19.1.1
- TypeScript
- Vite build system
- Chart.js for data visualization
- Axios for API communication

## Key Features

### 🔍 **Anomaly Detection Types**
1. **Late Night Queries** - Detects queries executed during unusual hours
2. **Large Data Dumps** - Identifies potential data exfiltration attempts
3. **Multi-Table Access** - Detects suspicious cross-table access patterns
4. **Sensitive Table Access** - Monitors access to sensitive data tables
5. **Unusual User Activity** - Identifies abnormal user behavior patterns
6. **AI-Powered Complexity Analysis** - LLM-based detection of complex anomalies

### 🗄️ **Database Support**
- **MySQL** - General query log parsing
- **PostgreSQL** - Log parsing and analysis
- **MongoDB** - Log parsing and analysis

### 🤖 **AI/ML Capabilities**
- **Isolation Forest** - Unsupervised anomaly detection
- **User-specific Models** - Personalized behavior analysis
- **LLM Integration** - Advanced pattern recognition using Ollama
- **Dual LLM System** - Enhanced analysis with multiple AI models

### 📊 **Data Visualization**
- Real-time anomaly statistics
- Interactive charts and graphs
- Detailed anomaly exploration
- Historical trend analysis

## Technology Stack

### Backend
- **Python 3.11+**
- **FastAPI** - Web framework
- **SQLAlchemy** - ORM
- **scikit-learn** - Machine learning
- **pandas** - Data processing
- **Ollama** - LLM integration

### Frontend
- **React 19.1.1**
- **TypeScript**
- **Vite** - Build tool
- **Chart.js** - Data visualization
- **Axios** - HTTP client

### Database
- **SQLite** - Primary database
- **PostgreSQL** - Optional for production

## Setup Instructions

### Prerequisites
- Python 3.11+
- Node.js
- Git
- PostgreSQL (optional)
- Ollama (optional, for LLM features)

### Installation
1. Clone the repository
2. Create Python virtual environment
3. Install Python dependencies: `pip install -r requirements.txt`
4. Install frontend dependencies: `cd uba-frontend && npm install`
5. Configure settings from templates
6. Run the application

### Running the Application
1. **Analysis Engine**: `python engine/engine_runner.py`
2. **Backend API**: `uvicorn backend_api.main_api:app --reload`
3. **Frontend**: `cd uba-frontend && npm run dev`

## Configuration

### Main Configuration (`config.py`)
- Database paths and settings
- Log file locations
- Anomaly detection parameters
- Email alert settings
- LLM configuration

### Engine Configuration (`engine_config.json`)
- Analysis parameters
- LLM settings
- Parser configurations
- Logging settings

## Security Features

- **Real-time Monitoring** - Continuous log analysis
- **Multi-layered Detection** - Rule-based + ML + LLM
- **User Behavior Profiling** - Individual user analysis
- **Email Alerts** - Immediate notification system
- **Configurable Rules** - Customizable detection parameters

This platform is designed for enterprise security monitoring and can be customized for specific organizational needs and compliance requirements.
