import { createRouter, createWebHistory } from "vue-router";

// Vamos criar esses componentes na próxima etapa
// Por enquanto, usamos importação dinâmica (lazy loading)
const routes = [
  {
    path: "/",
    name: "Dashboard",
    component: () => import("../views/DashboardView.vue"),
  },
  {
    path: "/operadoras",
    name: "Operadoras",
    component: () => import("../views/OperadorasView.vue"),
  },
  {
    path: "/operadoras/:cnpj", // Rota dinâmica (recebe o CNPJ)
    name: "DetalhesOperadora",
    component: () => import("../views/DetalhesView.vue"),
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;
