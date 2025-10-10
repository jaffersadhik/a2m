server {
        #listen       8080;
        listen        80 proxy_protocol;
        server_name   cm.itextos.com ;
        root         /usr/share/nginx/html;

        # Load configuration files for the default server block.
        include /etc/nginx/default.d/*.conf;

        location /campaigns/api/ {
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_pass_header Server;
            proxy_pass http://10.122.0.20:3000/;
            proxy_read_timeout 420s;
            proxy_connect_timeout 420s;
            root   html;
            index  index.html index.htm;
        }

        location /campaigns/uploads/FP-FileUpload-0.0.1/ {
            proxy_pass_header Server;
            proxy_pass http://10.122.0.20:8080/FP-FileUpload-0.0.1/;
            proxy_read_timeout 420s;
            proxy_connect_timeout 420s;
            root   html;
            index  index.html index.htm;
        }

       location /campaigns/uploads/FP-DltFileProcessor-0.0.1/ {
            proxy_pass_header Server;
            proxy_pass http://10.122.0.20:8080/FP-DltFileProcessor-0.0.1/;
            proxy_read_timeout 420s;
            proxy_connect_timeout 420s;
            root   html;
            index  index.html index.htm;
        }


        location /genericapi/ {
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_pass_header Server;
            proxy_pass http://10.122.0.20:8080/genericapi/;
            proxy_read_timeout 1200s;
            proxy_connect_timeout 1200s;
        }


        location /cm {
            alias /usr/share/nginx/html/cm;  # This is the path you're looking for
            index index.html;
        }


        location /logos {
            alias /usr/share/nginx/html/logos;  # This is the path you're looking for
            index index.html;
        }

       location /telcofiles {
            alias /usr/share/nginx/html/telcofiles;  # This is the path you're looking for
            index index.html;
        }


        error_page 404 /404.html;
        location = /404.html {
        }

        error_page 500 502 503 504 /50x.html;
        location = /50x.html {
        }
    }
