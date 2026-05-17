# Roadmap de Melhorias e Expansões (Possibilidades) — Sistema Distribuído Smart City

# 1. Objetivo do Documento

Este documento apresenta possíveis melhorias, correções estruturais e expansões arquiteturais planejadas para o sistema distribuído Smart City.

O objetivo é registrar:

* limitações identificadas;
* melhorias funcionais;
* evoluções arquiteturais;
* novos módulos;
* mecanismos de segurança;
* funcionalidades futuras.

As propostas foram organizadas por categoria para facilitar planejamento e desenvolvimento incremental.

---

# 2. Melhorias de Visualização e Monitoramento

# 2.1 Painel Individual de Dispositivos

## Objetivo

Adicionar um painel dedicado para análise individual de sensores.

---

## Funcionalidades Planejadas

O painel deverá permitir:

* histórico de métricas;
* status em tempo real;
* visualização de logs;
* métricas específicas do dispositivo;
* análise de falhas;
* controle remoto;
* visualização da comunicação.

---

## Benefícios

Essa funcionalidade melhora:

* observabilidade;
* depuração;
* gerenciamento operacional;
* monitoramento detalhado.

---

# 2.2 Painel Individual de Dispositivos

## Correções sobre os paineis do Streamlit

O painel deverá permitir:

* modificar informações de um sensor;
* corrigir painel estatistico;

# 3. Segurança e Defesa do Sistema

# 3.1 Sensor Penetra

## Objetivo

Criar um sensor especializado para testes de integridade e segurança.

---

## Função do Sensor

O sensor penetra deverá:

* validar segurança da comunicação;
* testar autenticação;
* analisar integridade da rede;
* detectar vulnerabilidades;
* validar mecanismos defensivos.

---

## Benefícios

Esse módulo transforma o projeto em uma plataforma mais próxima de:

* cyber-physical systems;
* ambientes resilientes;
* laboratórios de segurança distribuída.

---

# 3.2 Sistema de Defesa Gateway-Cliente

## Objetivo

Adicionar uma camada defensiva entre Gateway e cliente.

---

## Tecnologias Planejadas

Possíveis implementações utilizando alguma dessas opções:

* Rust;
* Java;
* Python;
* A escolha.

---

## Funções do Sistema Defensivo

O módulo poderá implementar:

* autenticação;
* autorização;
* IDS distribuído;
* validação de payloads;
* filtragem de tráfego;
* controle de acesso.

---

# 3.3 Criptografia entre Gateway e Sensores

## Objetivo

Adicionar criptografia na comunicação distribuída.

---

## Possíveis Tecnologias

Implementações previstas utilizando alguma dessas opções:

* Rust;
* Python;
* TLS;
* AES;
* criptografia assimétrica.

---

## Benefícios

Essa camada permitirá:

* proteção de telemetria;
* integridade de mensagens;
* autenticação de sensores;
* proteção contra interceptação.

---

# 3.4 Sistema de Ataque Simulado

## Objetivo

Adicionar um ambiente controlado para simulação de ataques.

---

## Cenários Possíveis

O sistema poderá simular:

* packet flooding;
* spoofing;
* replay attack;
* perda massiva de pacotes;
* saturação do Gateway;
* falhas distribuídas.

---

## Finalidade

Esse módulo permitirá:

* testes de resiliência;
* validação de segurança;
* análise de estabilidade;
* avaliação de tolerância a falhas.

---

# 4. Expansão de Sensores

# 4.1 Sistema de Cadastro de Sensores

## Objetivo

Criar um sistema dinâmico de gerenciamento de sensores.

---

## Funcionalidades

O sistema deverá permitir:

* cadastro;
* remoção;
* atualização;
* configuração;
* gerenciamento centralizado.

---

## Benefícios

Essa abordagem reduz:

* acoplamento;
* configuração manual;
* dependência de código fixo.

---

# 4.2 Envio de Configurações para Sensores

## Objetivo

Permitir que o Gateway envie configurações operacionais para os sensores.

---

## Configurações Possíveis

Exemplos:

* frequência de telemetria;
* limites de operação;
* parâmetros internos;
* modos de funcionamento;
* políticas de retry.

---

## Benefícios

Isso permitirá:

* reconfiguração dinâmica;
* controle centralizado;
* ajuste operacional em tempo real.

---

# 5. Controle de Acesso e Perfis

# 5.1 Sistema de Perfis

## Objetivo

Adicionar níveis de acesso ao sistema.

---

## Perfis Possíveis

Exemplos:

* administrador;
* operador;
* analista;
* auditor;
* usuário restrito.

---

## Funcionalidades

Cada perfil poderá controlar:

* visualização de informações;
* manipulação de sensores;
* acesso a logs;
* execução de comandos.

---

## Benefícios

Essa funcionalidade melhora:

* segurança;
* governança;
* rastreabilidade;
* controle operacional.

---

# 6. Backup e Continuidade Operacional

# 6.1 Sistema de Backup em GO/Outra Tecnologia

## Objetivo

Desenvolver um serviço em GO/Outra Tecnologia responsável por backup e recuperação.

---

## Funcionalidades Planejadas

O sistema deverá:

* salvar métricas;
* criar snapshots;
* recuperar serviços;
* monitorar disponibilidade;
* replicar informações críticas.

---

## Motivos para Uso de GO

GO oferece:

* alta concorrência;
* eficiência;
* baixo consumo;
* facilidade de deployment.

---

# 7. Persistência e Auditoria

# 7.1 Metadados Operacionais

## Objetivo

Salvar metadados relacionados às operações do sistema.

---

## Informações Planejadas

O sistema poderá registrar:

* comandos executados;
* falhas;
* eventos;
* conexões;
* operações críticas;
* atividade dos sensores.

---

## Benefícios

Essa funcionalidade permitirá:

* auditoria;
* rastreabilidade;
* debugging avançado;
* análise forense.

---

# 8. Expansão Web

# 8.1 Aplicação Web em Rust

## Objetivo

Produzir uma aplicação web utilizando Rust.

---

## Funcionalidades Esperadas

A aplicação poderá:

* monitorar sensores;
* visualizar métricas;
* enviar comandos;
* exibir topologia;
* controlar segurança;
* gerenciar usuários.

---

## Benefícios

Rust oferece:

* alta performance;
* segurança de memória;
* excelente concorrência;
* eficiência para aplicações web modernas.

---

# 9. Evolução Arquitetural

As melhorias propostas transformam o sistema atual em uma plataforma muito mais próxima de ambientes reais de:

* IoT distribuída;
* edge computing;
* cyber-physical systems;
* infraestrutura resiliente;
* sistemas críticos.

A evolução prevista amplia significativamente:

* segurança;
* observabilidade;
* escalabilidade;
* gerenciamento;
* resiliência;
* modularidade.


