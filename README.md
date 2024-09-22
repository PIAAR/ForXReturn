# RMoor Industries & Securities Forex Trading System

## Overview

Welcome to the Forex Trading System developed by RMoor Industries Ltd Co. This system is a comprehensive, modular platform designed for automated trading in the forex market. It includes real-time data processing, trading strategy execution, and a robust money management system. 

## Features

- **Real-Time Trading**: Connects to OANDA's API for real-time forex trading and market data.
- **Dynamic Trading Strategies**: Supports multiple trading strategies with real-time adjustments based on market conditions.
- **Advanced Money Management**: Flexible money management techniques that can be switched between low risk and high risk from the user interface.
- **Data Visualization**: Provides trend analysis with charts for monthly, daily, and minute data.
- **Modular Architecture**: Designed with a modular structure for easy maintenance and scalability.

## Directory Structure

```plaintext
/forex-trading-system
├── /backend
│   ├── /api
│   │   ├── /controllers
│   │   ├── /routes
│   │   ├── /services
│   │   └── /middleware
│   ├── /data
│   │   ├── /models
│   │   ├── /repositories
│   │   └── /utils
│   ├── /trading
│   │   ├── /strategies
│   │   ├── /optimizers
│   │   └── /managers
│   ├── /config
│   │   ├── /settings
│   │   └── /secrets
│   ├── /scripts
│   └── /tests
│       ├── /unit
│       └── /integration
│
├── /frontend
│   ├── /components
│   ├── /pages
│   ├── /styles
│   ├── /charts
│   ├── /hooks
│   ├── /utils
│   └── /public
│
├── /docs
│   ├── /architecture
│   ├── /api
│   ├── /user-manual
│   └── /technical
│
├── /scripts
│   ├── /data-import
│   ├── /setup
│   └── /maintenance
│
├── /configs
│   ├── /development
│   ├── /production
│   └── /testing
│
├── /docker
│   ├── /backend
│   ├── /frontend
│   └── /compose
│
├── /tests
│   ├── /unit
│   └── /integration
│
├── .gitignore
├── README.md
├── package.json
└── docker-compose.yml
```

## Getting Started

### Prerequisites

- Python 3.8+
- Node.js 14+
- Docker and Docker Compose
- OANDA API Key and Account ID

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/RMoorIndustries/forex-trading-system.git
   cd forex-trading-system
   ```

2. **Backend Setup**:
   - Navigate to the `backend` directory.
   - Create a virtual environment and install dependencies:
     ```bash
     cd backend
     python -m venv venv
     source venv/bin/activate
     pip install -r requirements.txt
     ```

3. **Frontend Setup**:
   - Navigate to the `frontend` directory and install dependencies:
     ```bash
     cd frontend
     npm install
     ```

4. **Configuration**:
   - Update `backend/config/secrets/secrets.py` with your OANDA API credentials and account details.
   - Set up environment variables or a configuration file for other settings.

5. **Run the Application**:
   - **Backend**: 
     ```bash
     cd backend
     flask run
     ```
   - **Frontend**:
     ```bash
     cd frontend
     npm start
     ```

   - Alternatively, use Docker Compose to build and run both services:
     ```bash
     docker-compose up --build
     ```

## Usage

- **Start Trading**: POST to `/api/start` to begin trading.
- **Stop Trading**: POST to `/api/stop` to halt trading.
- **Check Status**: GET `/api/status` to check the current trading status.

## Testing

- **Backend Tests**: Run unit and integration tests located in `backend/tests/`.
  ```bash
  cd backend
  pytest
  ```

- **Frontend Tests**: Run frontend tests using:
  ```bash
  cd frontend
  npm test
  ```

## Documentation

- **Architecture**: See `/docs/architecture` for system architecture diagrams and explanations.
- **API**: Refer to `/docs/api` for API endpoints and usage.
- **User Manual**: Find detailed usage instructions in `/docs/user-manual`.
- **Technical**: Explore `/docs/technical` for technical details and design decisions.

## Contributing

We welcome contributions to improve the Forex Trading System. Please follow these steps:

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/YourFeature`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Open a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For any questions or support, please contact us at [support@rmorindustries.com](mailto:support@rmorindustries.com).

---

Feel free to adjust any sections or add more details based on specific needs or additional functionality you may have.
