from EnterpriseAnalyticsPulse.DataAccess import AnalyticsRepository

from fastapi import FastAPI
app = FastAPI()

# Add services to the container.
pass # Configure controllers/routes (framework-specific setup in FastAPI)
analytics_repository = AnalyticsRepository() # Creates the BigQuery-enabled AnalyticsRepository instance as a singleton

app # Application instance is ready

pass # Apply HTTPS Redirection middleware
pass # Apply Authorization middleware
pass # Map defined API endpoints (controllers)

import uvicorn
uvicorn.run(app, host="0.0.0.0", port=8000)
