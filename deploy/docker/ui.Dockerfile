FROM node:20-alpine AS build

WORKDIR /app
COPY apps/ui/package*.json ./
RUN npm ci
COPY apps/ui ./
ARG VITE_API_BASE_URL=http://localhost:8000
ARG VITE_USE_MOCKS=false
ARG VITE_API_TOKEN=
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
ENV VITE_USE_MOCKS=$VITE_USE_MOCKS
ENV VITE_API_TOKEN=$VITE_API_TOKEN
RUN npm run build

FROM nginx:1.27-alpine

COPY --from=build /app/dist /usr/share/nginx/html
COPY deploy/docker/ui-nginx.conf /etc/nginx/conf.d/default.conf
