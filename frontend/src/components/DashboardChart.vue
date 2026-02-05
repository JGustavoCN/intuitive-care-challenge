<template>
  <Bar v-if="chartData.labels" :data="chartData" :options="chartOptions" />
  <div v-else class="text-center pa-4">Carregando gráfico...</div>
</template>

<script setup>
import { computed } from "vue";
import { Bar } from "vue-chartjs";
import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  BarElement,
  CategoryScale,
  LinearScale,
} from "chart.js";

// Registro dos módulos do Chart.js
ChartJS.register(
  Title,
  Tooltip,
  Legend,
  BarElement,
  CategoryScale,
  LinearScale,
);

// Recebemos os dados brutos da API como Propriedade (Props)
const props = defineProps({
  rawLabels: Array, // Ex: ['SP', 'RJ']
  rawValues: Array, // Ex: [1000, 500]
});

// Transformamos os dados no formato que o Chart.js entende
const chartData = computed(() => ({
  labels: props.rawLabels,
  datasets: [
    {
      label: "Despesas por UF (R$)",
      backgroundColor: "#1976D2",
      data: props.rawValues,
    },
  ],
}));

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
};
</script>
