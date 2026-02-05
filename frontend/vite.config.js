import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import vuetify from "vite-plugin-vuetify";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    // O autoImport carrega apenas os componentes que você usa (performance)
    vuetify({ autoImport: true }),
  ],
  server: {
    port: 3000, // Frontend rodará na porta 3000
  },
});
