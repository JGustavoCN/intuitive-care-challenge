import { createApp } from "vue";
import App from "./App.vue";

// Importar nossos plugins
import vuetify from "./plugins/vuetify";
import router from "./plugins/router";

const app = createApp(App);

// Registrar plugins
app.use(vuetify);
app.use(router);

app.mount("#app");
