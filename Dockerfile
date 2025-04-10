FROM node:18.20.2-alpine3.19

RUN mkdir -p /app && chown -R node:node /app
WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

USER node

COPY --chown=node:node . .

# Remove explicit PORT declaration
ENV NODE_ENV=production
EXPOSE $PORT  # Dynamic port exposure

CMD ["node", "server.js"]
