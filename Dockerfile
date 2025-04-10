FROM node:18.20.2-alpine3.19

# Create app directory with proper permissions
WORKDIR /app

# Install dependencies as root first
COPY package*.json ./
RUN npm ci --only=production

# Copy application files (with explicit file list)
COPY server.js google-sheets-sync.js ./

# Switch to non-root user
USER node

# Verify files exist (for debugging)
RUN ls -la /app && echo "Server.js exists: $(test -f /app/server.js && echo YES || echo NO)"

# Port configuration
EXPOSE 8080

CMD ["node", "server.js"]
