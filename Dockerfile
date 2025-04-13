FROM node:18.20.2-alpine3.19

# Create app directory
WORKDIR /app

# Copy application files
COPY package*.json ./
COPY server.js google-sheets-sync.js ./

# Install dependencies
RUN npm ci --only=production

# Create directory for mounted secrets
RUN mkdir -p /secrets

# Use non-root user for security
USER node

# Cloud Run will set PORT environment variable
EXPOSE 8080

CMD ["node", "server.js"]
