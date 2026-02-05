<template>
  <div>
    <h1 class="text-h4 mb-4">Lista de Operadoras</h1>

    <v-card>
      <v-card-title>
        <v-text-field
          v-model="search"
          append-inner-icon="mdi-magnify"
          label="Buscar por Razão Social ou CNPJ"
          single-line
          hide-details
          variant="outlined"
          density="compact"
          class="mb-4"
          @update:model-value="onSearch"
        ></v-text-field>
      </v-card-title>

      <v-data-table-server
        v-model:items-per-page="itemsPerPage"
        :headers="headers"
        :items="items"
        :items-length="totalItems"
        :loading="loading"
        :search="search"
        @update:options="loadItems"
        hover
        class="elevation-1"
      >
        <template v-slot:item.actions="{ item }">
          <v-btn
            size="small"
            color="primary"
            variant="text"
            :to="`/operadoras/${item.CNPJ}`"
          >
            Detalhes
          </v-btn>
        </template>
      </v-data-table-server>
    </v-card>
  </div>
</template>

<script setup>
import { ref } from "vue";
import api from "../plugins/axios";

// Configuração das Colunas
const headers = [
  { title: "Registro ANS", key: "RegistroANS", align: "start" },
  { title: "CNPJ", key: "CNPJ", align: "start" },
  {
    title: "Razão Social",
    key: "Razão Social",
    align: "start",
    key: "RazaoSocial",
  }, // Key deve bater com o JSON da API
  { title: "UF", key: "UF", align: "center" },
  { title: "Modalidade", key: "Modalidade", align: "start" },
  { title: "Ações", key: "actions", sortable: false, align: "end" },
];

// Estado Reativo
const items = ref([]);
const totalItems = ref(0);
const loading = ref(true);
const itemsPerPage = ref(10);
const search = ref("");
let searchTimeout = null;

// Função chamada automaticamente pelo Vuetify quando muda página/ordenação
const loadItems = async ({ page, itemsPerPage, sortBy }) => {
  loading.value = true;
  try {
    const response = await api.get("/operadoras", {
      params: {
        page: page,
        limit: itemsPerPage,
        search: search.value,
      },
    });

    // Atualiza tabela com dados da API
    items.value = response.data.data;
    totalItems.value = response.data.meta.total_items;
  } catch (error) {
    console.error("Erro ao carregar operadoras:", error);
  } finally {
    loading.value = false;
  }
};

// Debounce para a busca (espera 500ms antes de chamar a API)
const onSearch = () => {
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(() => {
    // Força o reload da tabela resetando para página 1
    loadItems({ page: 1, itemsPerPage: itemsPerPage.value });
  }, 500);
};
</script>
