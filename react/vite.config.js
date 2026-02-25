import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react({ include: /\.(js|jsx|ts|tsx)$/ })],
  server: {
    open: true,
  },
  build: {
    outDir: "build",
    emptyOutDir: true,
  },
});
