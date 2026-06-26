FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY src/ ./src/

FROM node:20-alpine
LABEL maintainer="acprompt-team"
WORKDIR /app
RUN addgroup -S busgroup && adduser -S bususer -G busgroup
COPY --from=builder /app ./
USER bususer
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD node -e "const ws=require('ws');const c=new ws('ws://localhost:8080/health');c.on('open',()=>{c.close();process.exit(0)});c.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),2000)"
ENTRYPOINT ["node", "src/server.js"]