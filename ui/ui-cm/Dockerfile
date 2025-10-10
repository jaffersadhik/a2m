FROM nginx:latest


ENV TZ="Asia/Kolkata"

COPY ./dist/cm /usr/share/nginx/html/cm

# Expose port 80
EXPOSE 80

# Start Nginx
CMD ["nginx", "-g", "daemon off;"]