FROM nginx:latest
ENV TZ="Asia/Kolkata"

# Copy built Angular app from the first stage to the Nginx image
COPY ./target /usr/share/nginx/
COPY ./target /usr/share/nginx/html


# Expose port 80
EXPOSE 80

# Start Nginx
CMD ["nginx", "-g", "daemon off;"]