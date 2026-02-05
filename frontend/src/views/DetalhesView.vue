<template>
  <div v-if="operadora">
    <v-btn
      to="/operadoras"
      variant="text"
      prepend-icon="mdi-arrow-left"
      class="mb-4"
    >
      Voltar
    </v-btn>

    <v-card class="mb-6">
      <v-card-title class="text-h5">
        {{ operadora.RazaoSocial }}
      </v-card-title>
      <v-card-subtitle class="text-subtitle-1 mb-2">
        CNPJ: {{ operadora.CNPJ }} | ANS: {{ operadora.RegistroANS }}
      </v-card-subtitle>
      <v-card-text>
        <v-chip class="mr-2" color="primary">{{ operadora.UF }}</v-chip>
        <v-chip variant="outlined">{{ operadora.Modalidade }}</v-chip>
      </v-card-text>
    </v-card>

    <h2 class="text-h5 mb-3">Histórico de Despesas</h2>

    <v-data-table
      :headers="headers"
      :items="despesas"
      :loading="loadingDespesas"
    >
      <template v-slot:item.ValorDespesas="{ item }">
        {{ formatMoney(item.ValorDespesas) }}
      </template>
    </v-data-table>
  </div>

  <div v-else-if="loading" class="text-center mt-10">
    <v-progress-circular indeterminate color="primary"></v-progress-circular>
    <p>Carregando dados da operadora...</p>
  </div>
</template>

<script setup>
import { ref, onMounted } from "vue";
import { useRoute } from "vue-router";
import api from "../plugins/axios";

const route = useRoute();
const cnpj = route.params.cnpj; // Pega o CNPJ da URL

const operadora = ref(null);
const despesas = ref([]);
const loading = ref(true);
const loadingDespesas = ref(true);

const headers = [
  { title: "Ano", key: "Ano" },
  { title: "Trimestre", key: "Trimestre" },
  { title: "Valor Despesa", key: "ValorDespesas", align: "end" },
];

const formatMoney = (value) => {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(value || 0);
};

onMounted(async () => {
  try {
    // 1. Busca dados cadastrais
    const respOp = await api.get(`/operadoras/${cnpj}`);
    operadora.value = respOp.data;
    loading.value = false;

    // 2. Busca histórico financeiro
    const respDespesas = await api.get(`/operadoras/${cnpj}/despesas`);
    despesas.value = respDespesas.data.data;
  } catch (error) {
    console.error("Erro ao carregar detalhes:", error);
    alert("Erro ao buscar detalhes.");
  } finally {
    loading.value = false;
    loadingDespesas.value = false;
  }
});
</script>
