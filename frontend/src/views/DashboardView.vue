<template>
  <div>
    <h1 class="text-h4 mb-4">Dashboard Financeiro</h1>

    <v-row>
      <v-col cols="12" md="4">
        <v-card color="primary" variant="tonal">
          <v-card-item title="Total de Despesas">
            <template v-slot:subtitle>
              <v-icon icon="mdi-cash" size="small" class="mr-1"></v-icon>
              Acumulado
            </template>
          </v-card-item>
          <v-card-text class="text-h5 font-weight-bold">
            {{ formatMoney(stats.total_despesas) }}
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="4">
        <v-card color="secondary" variant="tonal">
          <v-card-item title="Média Trimestral">
            <template v-slot:subtitle>
              <v-icon icon="mdi-chart-line" size="small" class="mr-1"></v-icon>
              Ticket Médio
            </template>
          </v-card-item>
          <v-card-text class="text-h5 font-weight-bold">
            {{ formatMoney(stats.media_trimestral) }}
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="4">
        <v-card color="info" variant="tonal">
          <v-card-item title="Operadoras Ativas">
            <template v-slot:subtitle>
              <v-icon
                icon="mdi-hospital-building"
                size="small"
                class="mr-1"
              ></v-icon>
              Total na Base
            </template>
          </v-card-item>
          <v-card-text class="text-h5 font-weight-bold">
            {{ stats.total_operadoras }}
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-row class="mt-4">
      <v-col cols="12">
        <v-card title="Top 5 Estados com Maiores Despesas">
          <v-card-text style="height: 300px">
            <DashboardChart
              v-if="!loading"
              :rawLabels="chartLabels"
              :rawValues="chartValues"
            />
            <v-progress-circular
              v-else
              indeterminate
              color="primary"
            ></v-progress-circular>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from "vue";
import api from "../plugins/axios";
import DashboardChart from "../components/DashboardChart.vue";

// Estado Reativo (Dados que mudam)
const stats = ref({
  total_despesas: 0,
  media_trimestral: 0,
  total_operadoras: 0,
  top_5_uf: [],
});
const loading = ref(true);

// Computed Properties para separar dados do gráfico
const chartLabels = computed(() => stats.value.top_5_uf.map((item) => item.UF));
const chartValues = computed(() =>
  stats.value.top_5_uf.map((item) => item.total),
);

// Função para formatar dinheiro (R$)
const formatMoney = (value) => {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(value || 0);
};

// Buscar dados ao carregar a página
onMounted(async () => {
  try {
    const response = await api.get("/estatisticas");
    stats.value = response.data;
  } catch (error) {
    console.error("Erro ao carregar dashboard:", error);
    alert("Erro ao conectar com a API. Verifique se o backend está rodando.");
  } finally {
    loading.value = false;
  }
});
</script>
