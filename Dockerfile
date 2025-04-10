FROM node:18.20.2-alpine3.19

# Create app directory with proper permissions
RUN mkdir -p /app && chown -R node:node /app
WORKDIR /app

# Install dependencies as root first
COPY package*.json ./
RUN npm ci --only=production

# Switch to non-root user
USER node

# Copy application files
COPY --chown=node:node . .

# Dynamic port exposure
EXPOSE $PORT

CMD ["node", "server.js"]
