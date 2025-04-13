# Use Node.js 18 Alpine image for lightweight and secure builds
FROM node:18.20.2-alpine3.19

# Create app directory
WORKDIR /app

# Copy application files
COPY package*.json ./
COPY server.js google-sheets-sync.js ./

# Install dependencies using npm install (instead of npm ci)
RUN npm install --only=production

# Create directory for mounted secrets
RUN mkdir -p /secrets

# Use non-root user for security
USER node

# Cloud Run will set PORT environment variable
EXPOSE 8080

# Start the application
CMD ["node", "server.js"]
