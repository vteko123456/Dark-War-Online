# DARK WAR online server — any container host (Fly.io, Railway, Cloud Run, a VPS…)
FROM node:20-alpine
WORKDIR /app
COPY server.js package.json ./
COPY public ./public
ENV PORT=8080
EXPOSE 8080
CMD ["node", "server.js"]
