FROM apify/actor-node-playwright-chrome:18 AS builder

WORKDIR /usr/src/app

USER root
RUN mkdir -p /usr/src/app && chown -R myuser:myuser /usr/src/app
USER myuser

COPY package*.json ./
RUN npm --quiet set fund false \
 && npm --quiet set audit false \
 && npm --quiet ci --omit=dev

COPY . ./

FROM apify/actor-node-playwright-chrome:18

WORKDIR /usr/src/app

USER root
RUN mkdir -p /usr/src/app && chown -R myuser:myuser /usr/src/app
USER myuser

COPY --from=builder /usr/src/app /usr/src/app

CMD ["node", "src/main.js"]


