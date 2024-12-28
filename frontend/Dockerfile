#front-end dockerfile

# Use the official Node.js image
FROM node:18

# Set working directory
WORKDIR /app

# Copy frontend files into the container
COPY . /app

# Install dependencies
RUN npm install

# Build the React app
RUN npm run build

# Expose the port used by the React app
EXPOSE 3000

# Command to serve the frontend using a simple HTTP server
RUN npm install -g serve
CMD ["serve", "-s", "build", "-l", "3000"]
