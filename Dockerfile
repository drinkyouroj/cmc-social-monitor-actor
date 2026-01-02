FROM apify/actor-node:18 AS builder

COPY package*.json ./
RUN npm --quiet set fund false \
 && npm --quiet set audit false \
 && npm --quiet ci --omit=dev

COPY . ./

FROM apify/actor-node:18

COPY --from=builder /usr/src/app /usr/src/app

CMD ["node", "src/main.js"]


