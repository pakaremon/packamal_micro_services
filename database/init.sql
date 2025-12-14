-- Initialize database for Pack-A-Mal development environment

-- Database is created automatically by Docker compose environment variables
-- This script runs additional setup if needed

-- Grant all privileges to ensure full access
GRANT ALL PRIVILEGES ON DATABASE packamal_dev TO pakaremon;

-- Enable required PostgreSQL extensions if needed
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Set timezone
SET timezone = 'UTC';

-- You can add additional initialization SQL here
-- For example, creating specific schemas, roles, etc.
