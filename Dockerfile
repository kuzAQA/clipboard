FROM node:24-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY relay.mjs ./
USER node
EXPOSE 18787
CMD ["node", "relay.mjs"]
