import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
import warnings
warnings.filterwarnings('ignore')

class CameraDataImputer:
    def __init__(self, db_path: str, target_client_locations: List[Tuple[str, str]] = None):
        """
        Sistema de imputação de dados de câmeras.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
            target_client_locations: Lista de tuplas (cliente, localização) para processar.
                                    Se None, processa todos os pares cliente-localização.
        """
        self.db_path = db_path
        self.target_client_locations = target_client_locations
        self.conn = None
        self.cameras_df = None
        self.flow_df = None
        self.weekday_columns = {
            0: ('counting_hour_monday', 'counting_hour_monday_qtd'),    # Segunda-feira
            1: ('counting_hour_tuesday', 'counting_hour_tuesday_qtd'),  # Terça-feira
            2: ('counting_hour_wednesday', 'counting_hour_wednesday_qtd'),  # Quarta-feira
            3: ('counting_hour_thursday', 'counting_hour_thursday_qtd'),    # Quinta-feira
            4: ('counting_hour_fryday', 'counting_hour_fryday_qtd'),        # Sexta-feira
            5: ('counting_hour_saturday', 'counting_hour_saturday_qtd'),    # Sábado
            6: ('counting_hour_sunday', 'counting_hour_sunday_qtd'),        # Domingo
        }
        
    def connect(self):
        """Estabelece conexão com o banco de dados."""
        self.conn = sqlite3.connect(self.db_path)
        
    def disconnect(self):
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            
    def get_client_location_list(self) -> List[Tuple[str, str]]:
        """
        Obtém lista de pares cliente-localização para processar.
        """
        if self.target_client_locations:
            return self.target_client_locations
            
        # Se nenhum alvo especificado, obtém todos do banco
        self.connect()
        query = """
            SELECT DISTINCT client, location 
            FROM login_camera 
            WHERE client IS NOT NULL AND location IS NOT NULL
            ORDER BY client, location
        """
        df = pd.read_sql_query(query, self.conn)
        self.disconnect()
        
        client_locations = list(df.itertuples(index=False, name=None))
        print(f"Encontrados {len(client_locations)} pares cliente-localização no banco de dados")
        return client_locations
    
    def load_data_for_client_location(self, client: str, location: str, days_back: int = 30) -> bool:
        """
        Carrega dados de câmeras e fluxo de pessoas para um cliente-localização específico.
        
        Args:
            client: Nome do cliente
            location: Nome da localização
            days_back: Número de dias para carregar dados históricos
            
        Returns:
            True se dados carregados com sucesso, False caso contrário
        """
        print(f"\n{'='*60}")
        print(f"Processando: {client} - {location}")
        print(f"{'='*60}")
        
        self.connect()
        
        # Carrega câmeras para este cliente-localização específico
        camera_query = """
            SELECT 
                id,
                client,
                location,
                pong_ts,
                counting_hour_sunday,
                counting_hour_sunday_qtd,
                counting_hour_monday,
                counting_hour_monday_qtd,
                counting_hour_tuesday,
                counting_hour_tuesday_qtd,
                counting_hour_wednesday,
                counting_hour_wednesday_qtd,
                counting_hour_thursday,
                counting_hour_thursday_qtd,
                counting_hour_fryday,
                counting_hour_fryday_qtd,
                counting_hour_saturday,
                counting_hour_saturday_qtd,
                counting_hour_holiday,
                counting_hour_holiday_qtd
            FROM login_camera
            WHERE client = ? AND location = ?
        """
        
        self.cameras_df = pd.read_sql_query(camera_query, self.conn, params=[client, location])
        
        if self.cameras_df.empty:
            print(f"Nenhuma câmera encontrada para {client} - {location}")
            self.disconnect()
            return False
            
        print(f"Carregadas {len(self.cameras_df)} câmeras para {client} - {location}")
        
        # Obtém IDs das câmeras para este cliente-localização
        target_camera_ids = self.cameras_df['id'].unique()
        
        # Calcula data de corte baseada em days_back
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Carrega totais de fluxo de pessoas para os últimos N dias, apenas para câmeras alvo
        placeholders = ','.join(['?'] * len(target_camera_ids))
        
        peopleflow_query = f"""
            SELECT id, created_at, camera_id, total_inside, total_outside, valid 
            FROM peopleflowtotals 
            WHERE created_at >= ? 
            AND camera_id IN ({placeholders})
            AND valid = 1
        """
        
        # Prepara parâmetros
        peopleflow_params = [cutoff_date] + target_camera_ids.tolist()
        
        self.flow_df = pd.read_sql_query(
            peopleflow_query, 
            self.conn, 
            params=peopleflow_params
        )
        
        # Converte colunas de data/hora
        if not self.flow_df.empty:
            self.flow_df['created_at'] = pd.to_datetime(self.flow_df['created_at'])
            self.flow_df['date'] = self.flow_df['created_at'].dt.date
            self.flow_df['hour'] = self.flow_df['created_at'].dt.hour
            self.flow_df['weekday'] = self.flow_df['created_at'].dt.weekday
            
            # Calcula intervalo de datas carregado
            min_date = self.flow_df['date'].min()
            max_date = self.flow_df['date'].max()
            date_range_days = (max_date - min_date).days + 1 if max_date != min_date else 1
            print(f"Carregados {len(self.flow_df)} registros de fluxo de {min_date} a {max_date} ({date_range_days} dias)")
        else:
            print(f"Nenhum dado de fluxo encontrado para {client} - {location}")
            return False
            
        return True
    
    def get_camera_active_hours(self, camera_id: int, weekday: int) -> Tuple[int, int]:
        """
        Obtém intervalo de horas ativas para uma câmera específica e dia da semana.
        
        Args:
            camera_id: ID da câmera
            weekday: Dia da semana (0=Segunda, 6=Domingo)
            
        Returns:
            Tupla de (hora_inicio, hora_fim)
        """
        if camera_id not in self.cameras_df['id'].values:
            return (0, 23)  # Padrão para todas as horas se câmera não encontrada
            
        camera_row = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
        
        if weekday in self.weekday_columns:
            start_col, end_col = self.weekday_columns[weekday]
            start_hour = camera_row[start_col]
            end_hour = camera_row[end_col]
            
            # Lida com valores None/NaN
            if pd.isna(start_hour) or pd.isna(end_hour):
                return (0, 23)
                
            # Garante intervalo válido
            start_hour = max(0, min(23, int(start_hour)))
            end_hour = max(0, min(23, int(end_hour)))
            
            return (start_hour, end_hour)
        else:
            return (0, 23)  # Padrão para todas as horas
        
    def get_last_valid_day(self) -> Optional[datetime]:
        """
        Obtém o último dia nos dados carregados que tem dados válidos (valid=1).
        
        Returns:
            Objeto datetime para o último dia válido, ou None se não houver dados válidos
        """
        if self.flow_df.empty:
            print("Nenhum dado válido disponível no dataset carregado.")
            return None
            
        # Obtém a data mais recente dos dados válidos
        last_date = self.flow_df['date'].max()
        last_datetime = datetime.combine(last_date, datetime.min.time())
        
        print(f"Último dia válido para este cliente-localização: {last_date}")
        
        return last_datetime
    







    def identify_failing_cameras(self, target_date: datetime = None) -> Dict[int, List[int]]:
        """
        Identifica câmeras com falha para o cliente-localização atual.
        
        MELHORIAS:
        1. Detecta valores 10x menores que o normal
        2. Adiciona múltiplos critérios de detecção
        3. Melhor logging para debug
        """
        if target_date is None:
            target_date = self.get_last_valid_day()
            
        if target_date is None:
            print("Nenhuma data alvo disponível. Não é possível identificar câmeras com falha.")
            return {}
            
        target_date_str = target_date.strftime('%Y-%m-%d')
        target_weekday = target_date.weekday()
        print(f"\nVerificando câmeras com falha em {target_date_str} (dia da semana: {target_weekday})")
        
        # Obtém todos os IDs de câmera para o cliente-localização atual
        camera_ids = self.cameras_df['id'].tolist()
        
        # Obtém dados para a data alvo
        target_data = self.flow_df[
            (self.flow_df['camera_id'].isin(camera_ids)) &
            (self.flow_df['date'] == target_date.date()) & 
            (self.flow_df['valid'] == 1)
        ]
        
        failing_cameras = {}
        
        for camera_id in camera_ids:
            # Obtém intervalo de horas ativas para esta câmera e dia da semana
            start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
            active_hours = list(range(start_hour, end_hour + 1))
            
            if not active_hours:
                continue
                
            camera_failed_hours = []
            
            for hour in active_hours:
                hour_data = target_data[
                    (target_data['camera_id'] == camera_id) &
                    (target_data['hour'] == hour)
                ]
                
                if hour_data.empty:
                    # Câmera não tem dados para esta hora ativa
                    camera_failed_hours.append(hour)
                    print(f"  ⚠️ Câmera {camera_id} hora {hour}: SEM DADOS")
                else:
                    # Verifica contagens anormalmente baixas
                    row = hour_data.iloc[0]
                    current_inside = row['total_inside']
                    current_outside = row['total_outside']
                    current_count = current_inside + current_outside
                    
                    # Obtém estatísticas históricas detalhadas
                    hist_stats = self._get_historical_statistics(camera_id, hour, target_weekday)
                    hist_avg = hist_stats['mean']
                    hist_std = hist_stats['std']
                    hist_min = hist_stats['min']
                    hist_max = hist_stats['max']
                    hist_count = hist_stats['count']
                    
                    # CRITÉRIO 1: Comparação com média histórica
                    # Se count > 0 para evitar divisão por zero
                    if hist_count >= 3 and hist_avg > 0:
                        ratio = current_count / hist_avg
                        
                        # Detectar valores muito baixos (10x menor = ratio = 0.1)
                        if ratio < 0.1:  # 10x menor que a média
                            camera_failed_hours.append(hour)
                            print(f"  ❌ Câmera {camera_id} hora {hour}: VALOR 10x MENOR")
                            print(f"     Atual: {current_count} (inside: {current_inside}, outside: {current_outside})")
                            print(f"     Média histórica: {hist_avg:.1f}")
                            print(f"     Ratio: {ratio:.3f} (esperado ~1.0)")
                            continue
                        
                        # CRITÉRIO 2: Menos que 20% da média (já existente)
                        if current_count < (hist_avg * 0.2):
                            camera_failed_hours.append(hour)
                            print(f"  ⚠️ Câmera {camera_id} hora {hour}: MENOS DE 20% DA MÉDIA")
                            print(f"     Atual: {current_count} vs Média: {hist_avg:.1f}")
                            continue
                        
                        # CRITÉRIO 3: Fora de 3 desvios padrão (para distribuição normal)
                        if hist_std > 0:
                            z_score = abs(current_count - hist_avg) / hist_std
                            if z_score > 3:
                                camera_failed_hours.append(hour)
                                print(f"  ⚠️ Câmera {camera_id} hora {hour}: FORA DE 3 DESVIOS PADRÃO")
                                print(f"     Z-score: {z_score:.2f}")
                                continue
                        
                        # CRITÉRIO 4: Verificar se inside/outside está balanceado
                        # Geralmente inside >= outside
                        if current_inside > 0 and current_outside > 0:
                            inside_ratio = current_inside / current_count
                            if inside_ratio < 0.3 or inside_ratio > 0.9:  # Fora do range normal
                                camera_failed_hours.append(hour)
                                print(f"  ⚠️ Câmera {camera_id} hora {hour}: BALANCEAMENTO ANORMAL")
                                print(f"     Inside ratio: {inside_ratio:.2f} (inside: {current_inside}, outside: {current_outside})")
                                continue
                        
                        # Se passou em todos os critérios
                        print(f"  ✅ Câmera {camera_id} hora {hour}: OK")
                        print(f"     Valor: {current_count}, Histórico: {hist_avg:.1f}±{hist_std:.1f}")
                    
                    else:
                        # Dados históricos insuficientes
                        print(f"  ℹ️ Câmera {camera_id} hora {hour}: DADOS HISTÓRICOS INSUFICIENTES")
                        print(f"     Registros históricos: {hist_count}")
                        print(f"     Valor atual: {current_count}")
                        
                        # Se não tem dados históricos mas o valor atual é muito baixo
                        if current_count < 10:  # Valor absoluto muito baixo
                            camera_failed_hours.append(hour)
                            print(f"     ⚠️ Marcado como falha: valor absoluto muito baixo")
            
            if camera_failed_hours:
                failing_cameras[camera_id] = camera_failed_hours
        
        # Imprime resumo
        print(f"\n{'='*60}")
        print(f"RESUMO: Encontradas {len(failing_cameras)} câmeras com falha")
        print(f"{'='*60}")
        
        for camera_id, hours in failing_cameras.items():
            camera_info = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
            start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
            print(f"  Câmera {camera_id} ({camera_info['location']}):")
            print(f"    Horas ativas: {start_hour}-{end_hour}")
            print(f"    Horas com falha: {hours}")
            
        return failing_cameras

    def _get_historical_statistics(self, camera_id: int, hour: int, weekday: int) -> Dict[str, float]:
        """
        Obtém estatísticas históricas detalhadas para câmera, hora e dia da semana.
        
        Returns:
            Dicionário com: mean, std, min, max, count, q1, q3
        """
        # Obtém dados históricos
        mask = (
            (self.flow_df['camera_id'] == camera_id) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        )
        
        historical_data = self.flow_df[mask]
        
        if len(historical_data) == 0:
            return {
                'mean': 0, 'std': 0, 'min': 0, 'max': 0, 
                'count': 0, 'q1': 0, 'q3': 0, 'median': 0
            }
        
        # Calcula tráfego total para cada registro
        total_traffic = historical_data['total_inside'] + historical_data['total_outside']
        
        # Estatísticas básicas
        stats = {
            'mean': total_traffic.mean(),
            'std': total_traffic.std(),
            'min': total_traffic.min(),
            'max': total_traffic.max(),
            'count': len(total_traffic),
            'median': total_traffic.median()
        }
        
        # Quartis (se tiver dados suficientes)
        if len(total_traffic) >= 4:
            stats['q1'] = total_traffic.quantile(0.25)
            stats['q3'] = total_traffic.quantile(0.75)
        else:
            stats['q1'] = stats['median']
            stats['q3'] = stats['median']
        
        return stats


    def _get_historical_average(self, camera_id: int, hour: int, weekday: int, 
                               weeks_back: int = 4) -> float:
        """Obtém média histórica de contagens para câmera, hora e dia da semana específicos."""
        # Obtém dados de semanas anteriores (mesmo dia da semana, mesma hora)
        mask = (
            (self.flow_df['camera_id'] == camera_id) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        )
        
        historical_data = self.flow_df[mask]
        
        if len(historical_data) == 0:
            return 0
            
        # Calcula tráfego total médio
        total_traffic = historical_data['total_inside'] + historical_data['total_outside']
        return total_traffic.mean()
    
    def _get_camera_relationships(self, target_weekday: int) -> Dict[int, Dict[int, float]]:
        """
        Calcula relações proporcionais entre câmeras dentro do cliente-localização atual.
        
        Args:
            target_weekday: Dia da semana para calcular relações
            
        Returns:
            Dicionário mapeando camera_id para dicionário de razões de outras câmeras
        """
        print(f"\nCalculando relações entre câmeras para dia da semana {target_weekday}...")
        
        camera_relationships = {}
        camera_ids = self.cameras_df['id'].tolist()
        
        # Calcula totais diários para cada câmera para o dia da semana alvo
        daily_totals = {}
        for camera_id in camera_ids:
            camera_data = self.flow_df[
                (self.flow_df['camera_id'] == camera_id) &
                (self.flow_df['weekday'] == target_weekday)
            ]
            
            if len(camera_data) > 0:
                daily_totals[camera_id] = {}
                for date in camera_data['date'].unique():
                    date_data = camera_data[camera_data['date'] == date]
                    daily_total = (date_data['total_inside'].sum() + 
                                  date_data['total_outside'].sum())
                    daily_totals[camera_id][date] = daily_total
        
        # Calcula razões entre câmeras
        for camera_id in camera_ids:
            camera_relationships[camera_id] = {}
            
            if camera_id not in daily_totals:
                continue
                
            for other_id in camera_ids:
                if other_id == camera_id or other_id not in daily_totals:
                    continue
                    
                # Encontra datas comuns
                common_dates = set(daily_totals[camera_id].keys()) & set(daily_totals[other_id].keys())
                if len(common_dates) >= 2:  # Precisa de pelo menos 2 datas comuns para razão confiável
                    ratios = []
                    for date in common_dates:
                        if daily_totals[camera_id][date] > 0:
                            ratio = daily_totals[other_id][date] / daily_totals[camera_id][date]
                            ratios.append(ratio)
                    
                    if ratios:
                        # Usa mediana para robustez contra outliers
                        camera_relationships[camera_id][other_id] = np.median(ratios)
        
        # Imprime resumo de relações
        cameras_with_relationships = len([c for c in camera_relationships if camera_relationships[c]])
        print(f"Calculadas relações para {cameras_with_relationships} câmeras")
        
        return camera_relationships
    
    def _get_weekday_patterns(self, camera_id: int) -> Dict[int, float]:
        """
        Obtém padrões de dias da semana para uma câmera.
        
        Returns:
            Dicionário mapeando dia da semana (0-6) para fator relativo
        """
        camera_data = self.flow_df[self.flow_df['camera_id'] == camera_id]
        
        if len(camera_data) == 0:
            return {i: 1.0 for i in range(7)}
        
        # Calcula tráfego médio por dia da semana
        weekday_totals = {}
        weekday_counts = {}
        
        for weekday in range(7):
            weekday_data = camera_data[camera_data['weekday'] == weekday]
            if len(weekday_data) > 0:
                # Conta apenas horas ativas para cada dia
                total_traffic = 0
                hour_count = 0
                
                for hour in range(24):
                    hour_data = weekday_data[weekday_data['hour'] == hour]
                    if not hour_data.empty:
                        # Obtém horas ativas para esta câmera e dia da semana
                        start_hour, end_hour = self.get_camera_active_hours(camera_id, weekday)
                        if start_hour <= hour <= end_hour:
                            total_traffic += hour_data['total_inside'].sum() + hour_data['total_outside'].sum()
                            hour_count += 1
                
                if hour_count > 0:
                    weekday_totals[weekday] = total_traffic
                    weekday_counts[weekday] = hour_count
        
        # Normaliza para obter fatores relativos
        if weekday_totals:
            # Calcula tráfego médio por hora ativa para cada dia da semana
            weekday_avg_per_hour = {wd: weekday_totals[wd]/weekday_counts[wd] 
                                   for wd in weekday_totals if weekday_counts[wd] > 0}
            
            if weekday_avg_per_hour:
                overall_avg = np.mean(list(weekday_avg_per_hour.values()))
                weekday_factors = {wd: avg/overall_avg for wd, avg in weekday_avg_per_hour.items()}
                
                # Preenche dias da semana faltantes com o mais próximo disponível
                for wd in range(7):
                    if wd not in weekday_factors:
                        # Encontra dia da semana mais próximo com dados
                        distances = [(abs(wd - other_wd), other_wd) 
                                    for other_wd in weekday_factors.keys()]
                        _, nearest_wd = min(distances)
                        weekday_factors[wd] = weekday_factors[nearest_wd]
                        
                return weekday_factors
        
        return {i: 1.0 for i in range(7)}
    
    def estimate_missing_data(self, failing_cameras: Dict[int, List[int]], 
                            target_date: datetime) -> pd.DataFrame:
        """
        Estima dados ausentes para câmeras com falha - VERSÃO REVISADA.
        
        CORREÇÕES:
        1. Verifica se hist_ratio está sendo calculado corretamente
        2. Adiciona validação das estimativas
        3. Usa média ponderada quando múltiplas câmeras de referência
        """
        print(f"\nEstimando dados ausentes para {target_date.date()}...")
        
        target_weekday = target_date.weekday()
        
        # DEBUG: Mostrar quais câmeras estão falhando
        print(f"Câmeras com falha: {list(failing_cameras.keys())}")
        
        # Primeiro, verifique se há câmeras funcionando disponíveis
        all_camera_ids = self.cameras_df['id'].tolist()
        working_cameras = [cam_id for cam_id in all_camera_ids if cam_id not in failing_cameras]
        
        if len(working_cameras) == 0:
            print("⚠️  NENHUMA câmera funcionando disponível! Usando apenas dados históricos próprios.")
            # Usa apenas estimativa histórica própria
            return self._estimate_all_from_own_history(failing_cameras, target_date)
        
        print(f"Câmeras funcionando disponíveis: {working_cameras}")
        
        estimated_records = []
        
        for camera_id, missing_hours in failing_cameras.items():
            print(f"\n{'='*40}")
            print(f"PROCESSANDO CÂMERA {camera_id}")
            print(f"{'='*40}")
            
            # Obtém intervalo de horas ativas
            start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
            active_hours = set(range(start_hour, end_hour + 1))
            missing_hours = [h for h in missing_hours if h in active_hours]
            
            if not missing_hours:
                continue
            
            print(f"Horas a estimar: {missing_hours}")
            
            # Obter dados históricos da própria câmera como baseline
            own_history_estimates = self._get_own_history_baseline(camera_id, missing_hours, target_date)
            print(f"Baseline histórico próprio: {own_history_estimates}")
            
            for hour in missing_hours:
                print(f"\n  Hora {hour:02d}:")
                
                # Tentar usar câmeras de referência
                reference_estimates = []
                reference_weights = []
                
                for other_id in working_cameras:
                    # Verificar se a câmera de referência está ativa nesta hora
                    other_start, other_end = self.get_camera_active_hours(other_id, target_weekday)
                    if not (other_start <= hour <= other_end):
                        continue
                    
                    # Verificar se tem dados para esta hora
                    other_hour_data = self.flow_df[
                        (self.flow_df['camera_id'] == other_id) &
                        (self.flow_df['date'] == target_date.date()) &
                        (self.flow_df['hour'] == hour) &
                        (self.flow_df['valid'] == 1)
                    ]
                    
                    if other_hour_data.empty:
                        continue
                    
                    other_row = other_hour_data.iloc[0]
                    other_inside = other_row['total_inside']
                    other_outside = other_row['total_outside']
                    other_total = other_inside + other_outside
                    
                    # Calcular razão histórica CORRETAMENTE
                    hist_ratio = self._get_hourly_ratio(camera_id, other_id, hour, target_weekday)
                    
                    if hist_ratio > 0:
                        # IMPORTANTE: A razão é camera_b / camera_a
                        # Se hist_ratio = 2.0, significa que other_id tem 2x mais que camera_id
                        # Então: camera_id_estimate = other_value / hist_ratio
                        
                        estimated_inside = int(other_inside / hist_ratio)
                        estimated_outside = int(other_outside / hist_ratio)
                        
                        print(f"    Referência Câmera {other_id}:")
                        print(f"      Valores: {other_inside}/{other_outside}")
                        print(f"      Razão histórica: {hist_ratio:.3f}")
                        print(f"      Estimativa: {estimated_inside}/{estimated_outside}")
                        
                        reference_estimates.append((estimated_inside, estimated_outside))
                        
                        # Peso baseado na confiança da razão histórica
                        # Quanto mais dados históricos, maior o peso
                        confidence = self._get_ratio_confidence(camera_id, other_id, hour, target_weekday)
                        reference_weights.append(confidence)
                
                # Combinar estimativas
                final_inside, final_outside = self._combine_estimates(
                    reference_estimates, reference_weights, 
                    own_history_estimates.get(hour, (0, 0)), hour
                )
                
                # Garantir valores razoáveis
                final_inside, final_outside = self._apply_sanity_checks(
                    final_inside, final_outside, camera_id, hour, target_weekday
                )
                
                # Criar registro
                hour_timestamp = datetime(
                    target_date.year, target_date.month, target_date.day, hour
                )
                
                record = {
                    'created_at': hour_timestamp,
                    'camera_id': camera_id,
                    'total_inside': final_inside,
                    'total_outside': final_outside,
                    'valid': 1,
                    'estimated': 1,
                    'client': self.cameras_df[self.cameras_df['id'] == camera_id]['client'].iloc[0],
                    'location': self.cameras_df[self.cameras_df['id'] == camera_id]['location'].iloc[0]
                }
                
                estimated_records.append(record)
                
                print(f"    ✅ Estimativa final: {final_inside}/{final_outside}")
        
        return pd.DataFrame(estimated_records)

    def _get_own_history_baseline(self, camera_id: int, missing_hours: List[int], 
                                target_date: datetime) -> Dict[int, Tuple[int, int]]:
        """Obtém baseline histórica da própria câmera."""
        target_weekday = target_date.weekday()
        weekday_factors = self._get_weekday_patterns(camera_id)
        target_factor = weekday_factors[target_weekday]
        
        baseline = {}
        
        for hour in missing_hours:
            hist_data = self.flow_df[
                (self.flow_df['camera_id'] == camera_id) &
                (self.flow_df['hour'] == hour) &
                (self.flow_df['weekday'] == target_weekday)
            ]
            
            if len(hist_data) >= 2:
                avg_inside = hist_data['total_inside'].mean()
                avg_outside = hist_data['total_outside'].mean()
                
                estimated_inside = int(avg_inside * target_factor)
                estimated_outside = int(avg_outside * target_factor)
                
                baseline[hour] = (estimated_inside, estimated_outside)
        
        return baseline

    def _get_hourly_ratio(self, camera_a: int, camera_b: int, hour: int, weekday: int) -> float:
        """
        Obtém razão histórica entre duas câmeras para hora e dia da semana específicos.
        
        RETORNA: camera_b_total / camera_a_total
        Se ratio = 2.0, camera_b tem 2x mais movimento que camera_a
        """
        # Obtém dados históricos para ambas as câmeras
        data_a = self.flow_df[
            (self.flow_df['camera_id'] == camera_a) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        data_b = self.flow_df[
            (self.flow_df['camera_id'] == camera_b) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        if len(data_a) == 0 or len(data_b) == 0:
            return 0
        
        # Encontra datas comuns
        dates_a = set(data_a['date'].unique())
        dates_b = set(data_b['date'].unique())
        common_dates = dates_a & dates_b
        
        if len(common_dates) < 2:
            return 0
        
        # Calcula razões para datas comuns
        ratios = []
        for date in common_dates:
            total_a = (data_a[data_a['date'] == date]['total_inside'].sum() + 
                    data_a[data_a['date'] == date]['total_outside'].sum())
            total_b = (data_b[data_b['date'] == date]['total_inside'].sum() + 
                    data_b[data_b['date'] == date]['total_outside'].sum())
            
            if total_a > 0:
                ratio = total_b / total_a
                # Verificar se ratio é razoável (evitar outliers)
                if 0.1 < ratio < 10:  # Limites razoáveis
                    ratios.append(ratio)
        
        if not ratios:
            return 0
        
        # DEBUG: Mostrar estatísticas da razão
        print(f"      Razões calculadas: {len(ratios)} valores")
        print(f"      Média: {np.mean(ratios):.3f}, Mediana: {np.median(ratios):.3f}")
        print(f"      Min: {np.min(ratios):.3f}, Max: {np.max(ratios):.3f}")
        
        return np.median(ratios)

    def _get_ratio_confidence(self, camera_a: int, camera_b: int, hour: int, weekday: int) -> float:
        """Calcula confiança na razão histórica (0-1)."""
        data_a = self.flow_df[
            (self.flow_df['camera_id'] == camera_a) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        data_b = self.flow_df[
            (self.flow_df['camera_id'] == camera_b) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        dates_a = set(data_a['date'].unique())
        dates_b = set(data_b['date'].unique())
        common_dates = dates_a & dates_b
        
        if len(common_dates) < 2:
            return 0
        
        # Confiança baseada no número de datas comuns
        confidence = min(len(common_dates) / 10, 1.0)  # Máximo 1.0
        
        # Penalizar se a variação for muito grande
        ratios = []
        for date in common_dates:
            total_a = (data_a[data_a['date'] == date]['total_inside'].sum() + 
                    data_a[data_a['date'] == date]['total_outside'].sum())
            total_b = (data_b[data_b['date'] == date]['total_inside'].sum() + 
                    data_b[data_b['date'] == date]['total_outside'].sum())
            
            if total_a > 0:
                ratios.append(total_b / total_a)
        
        if len(ratios) >= 3:
            cv = np.std(ratios) / np.mean(ratios)  # Coeficiente de variação
            if cv > 0.5:  # Variação muito alta
                confidence *= 0.5
        
        return confidence

    def _combine_estimates(self, reference_estimates: List[Tuple[int, int]], 
                        reference_weights: List[float],
                        own_history_estimate: Tuple[int, int],
                        hour: int) -> Tuple[int, int]:
        """Combina múltiplas estimativas."""
        
        if not reference_estimates:
            # Sem referências, usar histórico próprio
            return own_history_estimate
        
        # Normalizar pesos
        total_weight = sum(reference_weights)
        if total_weight == 0:
            return own_history_estimate
        
        normalized_weights = [w/total_weight for w in reference_weights]
        
        # Calcular média ponderada
        weighted_inside = sum(est[0] * weight for est, weight in zip(reference_estimates, normalized_weights))
        weighted_outside = sum(est[1] * weight for est, weight in zip(reference_estimates, normalized_weights))
        
        # Se tivermos estimativa própria, fazer média ponderada com ela também
        if own_history_estimate != (0, 0):
            # Dar peso menor ao histórico próprio (0.3) vs referências (0.7)
            final_inside = int(weighted_inside * 0.7 + own_history_estimate[0] * 0.3)
            final_outside = int(weighted_outside * 0.7 + own_history_estimate[1] * 0.3)
        else:
            final_inside = int(weighted_inside)
            final_outside = int(weighted_outside)
        
        return final_inside, final_outside

    def _apply_sanity_checks(self, estimated_inside: int, estimated_outside: int,
                            camera_id: int, hour: int, weekday: int) -> Tuple[int, int]:
        """Aplica verificações de sanidade às estimativas."""
        
        # 1. Obter estatísticas históricas para limites
        hist_data = self.flow_df[
            (self.flow_df['camera_id'] == camera_id) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        if len(hist_data) >= 3:
            hist_inside = hist_data['total_inside']
            hist_outside = hist_data['total_outside']
            
            # Calcular percentis
            inside_q1 = hist_inside.quantile(0.25)
            inside_q3 = hist_inside.quantile(0.75)
            inside_iqr = inside_q3 - inside_q1
            inside_lower = max(0, inside_q1 - 1.5 * inside_iqr)
            inside_upper = inside_q3 + 1.5 * inside_iqr
            
            outside_q1 = hist_outside.quantile(0.25)
            outside_q3 = hist_outside.quantile(0.75)
            outside_iqr = outside_q3 - outside_q1
            outside_lower = max(0, outside_q1 - 1.5 * outside_iqr)
            outside_upper = outside_q3 + 1.5 * outside_iqr
            
            # Aplicar limites
            estimated_inside = int(max(inside_lower, min(estimated_inside, inside_upper)))
            estimated_outside = int(max(outside_lower, min(estimated_outside, outside_upper)))
        
        # 2. Garantir que entrada >= saída (geralmente verdadeiro)
        if estimated_inside < estimated_outside:
            # Ajustar mantendo a proporção
            total = estimated_inside + estimated_outside
            estimated_inside = int(total * 0.55)  # 55% entrada, 45% saída
            estimated_outside = total - estimated_inside
        
        # 3. Garantir valores não negativos
        estimated_inside = max(0, estimated_inside)
        estimated_outside = max(0, estimated_outside)
        
        return estimated_inside, estimated_outside

    def _estimate_all_from_own_history(self, failing_cameras: Dict[int, List[int]], 
                                    target_date: datetime) -> pd.DataFrame:
        """Estima todas as câmeras apenas com dados históricos próprios."""
        print("USANDO APENAS DADOS HISTÓRICOS PRÓPRIOS")
        
        estimated_records = []
        target_weekday = target_date.weekday()
        
        for camera_id, missing_hours in failing_cameras.items():
            weekday_factors = self._get_weekday_patterns(camera_id)
            target_factor = weekday_factors[target_weekday]
            
            for hour in missing_hours:
                hist_data = self.flow_df[
                    (self.flow_df['camera_id'] == camera_id) &
                    (self.flow_df['hour'] == hour) &
                    (self.flow_df['weekday'] == target_weekday)
                ]
                
                if len(hist_data) >= 2:
                    avg_inside = hist_data['total_inside'].mean()
                    avg_outside = hist_data['total_outside'].mean()
                    
                    estimated_inside = int(avg_inside * target_factor)
                    estimated_outside = int(avg_outside * target_factor)
                else:
                    # Sem dados históricos suficientes
                    # Usar média de horas ativas similares
                    similar_hours = self.flow_df[
                        (self.flow_df['camera_id'] == camera_id) &
                        (self.flow_df['weekday'] == target_weekday)
                    ]
                    
                    if len(similar_hours) > 0:
                        estimated_inside = int(similar_hours['total_inside'].mean() * target_factor)
                        estimated_outside = int(similar_hours['total_outside'].mean() * target_factor)
                    else:
                        # Último recurso: usar valores padrão baseados na hora
                        estimated_inside = self._get_default_value(hour, 'inside')
                        estimated_outside = self._get_default_value(hour, 'outside')
                
                # Aplicar verificações de sanidade
                estimated_inside, estimated_outside = self._apply_sanity_checks(
                    estimated_inside, estimated_outside, camera_id, hour, target_weekday
                )
                
                hour_timestamp = datetime(
                    target_date.year, target_date.month, target_date.day, hour
                )
                
                record = {
                    'created_at': hour_timestamp,
                    'camera_id': camera_id,
                    'total_inside': estimated_inside,
                    'total_outside': estimated_outside,
                    'valid': 1,
                    'estimated': 1,
                    'client': self.cameras_df[self.cameras_df['id'] == camera_id]['client'].iloc[0],
                    'location': self.cameras_df[self.cameras_df['id'] == camera_id]['location'].iloc[0]
                }
                
                estimated_records.append(record)
                
                print(f"Câmera {camera_id}, Hora {hour:02d}: {estimated_inside}/{estimated_outside}")
        
        return pd.DataFrame(estimated_records)

    def _estimate_from_own_history(self, camera_id: int, missing_hours: List[int], 
                                  target_date: datetime, target_factor: float,
                                  estimated_records: List[Dict]):
        """Estima dados usando padrões históricos da própria câmera."""
        print(f"  Usando padrões históricos para Câmera {camera_id}")
        
        target_weekday = target_date.weekday()
        camera_info = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
        client = camera_info['client']
        location = camera_info['location']
        
        for hour in missing_hours:
            # Obtém média histórica para esta câmera, hora e dia da semana
            hist_data = self.flow_df[
                (self.flow_df['camera_id'] == camera_id) &
                (self.flow_df['hour'] == hour) &
                (self.flow_df['weekday'] == target_weekday)
            ]
            
            if len(hist_data) >= 2:  # Precisa de pelo menos 2 pontos históricos
                # Calcula média entrada/saída
                avg_inside = hist_data['total_inside'].mean()
                avg_outside = hist_data['total_outside'].mean()
                
                # Ajusta pelo fator do dia da semana
                estimated_inside = int(avg_inside * target_factor)
                estimated_outside = int(avg_outside * target_factor)
                
                # Garante não negativo
                estimated_inside = max(0, estimated_inside)
                estimated_outside = max(0, estimated_outside)
                
                # Cria timestamp
                hour_timestamp = datetime(
                    target_date.year, target_date.month, target_date.day, hour
                )
                
                record = {
                    'created_at': hour_timestamp,
                    'camera_id': camera_id,
                    'total_inside': estimated_inside,
                    'total_outside': estimated_outside,
                    'valid': 1,
                    'estimated': 1,
                    'client': client,
                    'location': location
                }
                
                estimated_records.append(record)
                print(f"  Hora {hour:02d}: Estimativa histórica {estimated_inside} entrada, {estimated_outside} saída")
            else:
                print(f"  Hora {hour:02d}: Dados históricos insuficientes")
    
    def _estimate_hour_from_history(self, camera_id: int, hour: int, 
                                   target_date: datetime, target_factor: float,
                                   estimated_records: List[Dict]):
        """Estima hora única a partir do histórico da própria câmera."""
        target_weekday = target_date.weekday()
        camera_info = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
        client = camera_info['client']
        location = camera_info['location']
        
        # Obtém média histórica para esta câmera, hora e dia da semana
        hist_data = self.flow_df[
            (self.flow_df['camera_id'] == camera_id) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == target_weekday)
        ]
        
        if len(hist_data) >= 2:
            avg_inside = hist_data['total_inside'].mean()
            avg_outside = hist_data['total_outside'].mean()
            
            estimated_inside = int(avg_inside * target_factor)
            estimated_outside = int(avg_outside * target_factor)
            
            # Garante não negativo
            estimated_inside = max(0, estimated_inside)
            estimated_outside = max(0, estimated_outside)
            
            hour_timestamp = datetime(
                target_date.year, target_date.month, target_date.day, hour
            )
            
            record = {
                'created_at': hour_timestamp,
                'camera_id': camera_id,
                'total_inside': estimated_inside,
                'total_outside': estimated_outside,
                'valid': 1,
                'estimated': 1,
                'client': client,
                'location': location
            }
            
            estimated_records.append(record)
            print(f"  Hora {hour:02d}: Fallback histórico {estimated_inside} entrada, {estimated_outside} saída")
        else:
            print(f"  Hora {hour:02d}: Nenhum dado disponível para estimativa")
    
    def _get_hourly_ratio(self, camera_a: int, camera_b: int, hour: int, weekday: int) -> float:
        """Obtém razão histórica entre duas câmeras para hora e dia da semana específicos."""
        # Obtém dados históricos para ambas as câmeras
        data_a = self.flow_df[
            (self.flow_df['camera_id'] == camera_a) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        data_b = self.flow_df[
            (self.flow_df['camera_id'] == camera_b) &
            (self.flow_df['hour'] == hour) &
            (self.flow_df['weekday'] == weekday)
        ]
        
        if len(data_a) == 0 or len(data_b) == 0:
            return 0
        
        # Encontra datas comuns
        dates_a = set(data_a['date'].unique())
        dates_b = set(data_b['date'].unique())
        common_dates = dates_a & dates_b
        
        if len(common_dates) < 2:
            return 0
        
        # Calcula razões para datas comuns
        ratios = []
        for date in common_dates:
            total_a = (data_a[data_a['date'] == date]['total_inside'].sum() + 
                      data_a[data_a['date'] == date]['total_outside'].sum())
            total_b = (data_b[data_b['date'] == date]['total_inside'].sum() + 
                      data_b[data_b['date'] == date]['total_outside'].sum())
            
            if total_a > 0:
                ratios.append(total_b / total_a)
        
        return np.median(ratios) if ratios else 0
    
    def convert_timestamp_for_sqlite(self, timestamp_value):
        """
        Converte pandas Timestamp, Python datetime ou string para string compatível com SQLite.
        """
        if pd.isna(timestamp_value):
            return None
        elif isinstance(timestamp_value, pd.Timestamp):
            return timestamp_value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(timestamp_value, datetime):
            return timestamp_value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(timestamp_value, str):
            # Verifica se já está no formato correto
            try:
                datetime.strptime(timestamp_value, '%Y-%m-%d %H:%M:%S')
                return timestamp_value
            except ValueError:
                # Tenta converter se estiver em outro formato
                try:
                    dt = pd.to_datetime(timestamp_value)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    return timestamp_value
        else:
            return str(timestamp_value)
    
    def insert_estimated_data(self, estimated_df: pd.DataFrame) -> Tuple[int, int]:
        """
        Insere dados estimados no banco de dados.
        
        CORREÇÃO: Lida corretamente com múltiplas câmeras no mesmo datetime.
        
        Returns:
            Tupla de (inserted_count, updated_count)
        """
        if estimated_df.empty:
            print("\nNenhum dado estimado para inserir.")
            return 0, 0
        
        cursor = self.conn.cursor()
        
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        # Ordena por camera_id para consistência
        estimated_df = estimated_df.sort_values('camera_id')
        
        print(f"Processando {len(estimated_df)} registros estimados...")
        
        for i, (_, row) in enumerate(estimated_df.iterrows(), 1):
            # Converte Timestamp para string compatível com SQLite
            created_at_sql = self.convert_timestamp_for_sqlite(row['created_at'])
            camera_id = int(row['camera_id'])
            
            if i % 100 == 0:  # Log de progresso
                print(f"  Processando registro {i}/{len(estimated_df)}...")
            
            try:
                # Verifica se registro já existe (combinando camera_id E created_at)
                cursor.execute("""
                    SELECT id, valid FROM peopleflowtotals 
                    WHERE camera_id = ? AND created_at = ?
                """, (camera_id, created_at_sql))
                
                existing = cursor.fetchone()
                
                if existing is None:
                        cursor.execute("""
                            INSERT INTO peopleflowtotals 
                            (created_at, camera_id, total_inside, total_outside, valid)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            created_at_sql,
                            camera_id,
                            int(row['total_inside']),
                            int(row['total_outside']),
                            1  # Marca como válido
                        ))
                        inserted_count += 1        
                else:
                    # Atualiza registro inválido existente
                    existing_id, _ = existing
                    cursor.execute("""
                        UPDATE peopleflowtotals 
                        SET total_inside = ?, total_outside = ?, valid = 1
                        WHERE id = ?
                    """, (
                        int(row['total_inside']),
                        int(row['total_outside']),
                        existing_id
                    ))
                    updated_count += 1
            
            except Exception as e:
                print(f"\n❌ Erro processando registro {i}:")
                print(f"   Câmera: {camera_id}, Data/hora: {created_at_sql}")
                print(f"   Erro: {e}")
                # Continua com próximo registro
                continue
        
        self.conn.commit()
        
        print(f"\nResumo da inserção:")
        print(f"  ✅ Inseridos: {inserted_count} novos registros")
        print(f"  🔄 Atualizados: {updated_count} registros existentes")
        print(f"  ⏭️ Pulados: {skipped_count} registros (já válidos)")
        
        return inserted_count, updated_count
    
    def create_imputation_log(self, client: str, location: str, target_date: datetime,
                            estimated_df: pd.DataFrame, inserted: int, updated: int):
        """Cria entrada de log para o processo de imputação."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_imputation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    imputation_date TIMESTAMP,
                    client TEXT,
                    location TEXT,
                    target_date DATE,
                    target_weekday INTEGER,
                    cameras_affected INTEGER,
                    hours_estimated INTEGER,
                    records_inserted INTEGER,
                    records_updated INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cameras_affected = len(estimated_df['camera_id'].unique())
            hours_estimated = len(estimated_df)
            
            notes = f"Imputados dados para {cameras_affected} câmeras, {hours_estimated} horas"
            
            cursor.execute("""
                INSERT INTO data_imputation_log 
                (imputation_date, client, location, target_date, target_weekday,
                 cameras_affected, hours_estimated, records_inserted, records_updated, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                client,
                location,
                target_date.strftime('%Y-%m-%d'),
                target_date.weekday(),
                cameras_affected,
                hours_estimated,
                inserted,
                updated,
                notes
            ))
            
            self.conn.commit()
            print("Log de imputação criado com sucesso.")
        except Exception as e:
            print(f"Nota: Não foi possível criar log de imputação: {e}")
    
    def process_client_location(self, client: str, location: str, days_back: int = 45) -> Dict:
        """
        Processa um par cliente-localização único.
        
        Args:
            client: Nome do cliente
            location: Nome da localização
            days_back: Número de dias para usar dados históricos
            
        Returns:
            Dicionário com resultados do processamento
        """
        results = {
            'client': client,
            'location': location,
            'success': False,
            'cameras_loaded': 0,
            'failing_cameras': 0,
            'hours_estimated': 0,
            'records_inserted': 0,
            'records_updated': 0
        }
        
        # Carrega dados para este cliente-localização
        if not self.load_data_for_client_location(client, location, days_back):
            return results
            
        results['cameras_loaded'] = len(self.cameras_df)
        
        # Obtém o último dia válido dos dados carregados
        target_date = self.get_last_valid_day()
        
        if target_date is None:
            print("Nenhum dado válido encontrado. Pulando este cliente-localização.")
            self.disconnect()
            return results
        
        # Identifica câmeras com falha
        failing_cameras = self.identify_failing_cameras(target_date)
        results['failing_cameras'] = len(failing_cameras)
        
        if not failing_cameras:
            print("\nNenhuma câmera com falha detectada. Nada a fazer.")
            self.disconnect()
            results['success'] = True
            return results
        
        # Estima dados ausentes
        estimated_data = self.estimate_missing_data(failing_cameras, target_date)
        results['hours_estimated'] = len(estimated_data)
        
        if estimated_data.empty:
            print("\nNão foi possível estimar nenhum dado ausente.")
            self.disconnect()
            results['success'] = True
            return results
        
        # Insere dados estimados
        inserted, updated = self.insert_estimated_data(estimated_data)
        results['records_inserted'] = inserted
        results['records_updated'] = updated
        
        # Cria log de imputação
        if inserted > 0 or updated > 0:
            self.create_imputation_log(client, location, target_date, estimated_data, inserted, updated)
        
        # Limpeza
        self.disconnect()
        results['success'] = True
        
        return results
    
    def run_imputation(self, days_back: int = 45):
        """
        Método principal para executar o processo completo de imputação para todos clientes-localizações.
        
        Args:
            days_back: Número de dias para usar dados históricos
        """
        print("=" * 60)
        print("SISTEMA DE IMPUTAÇÃO DE DADOS DE CÂMERAS")
        print("=" * 60)
        
        # Obtém lista de clientes-localizações para processar
        client_locations = self.get_client_location_list()
        
        if not client_locations:
            print("Nenhum cliente-localização para processar.")
            return
        
        print(f"\nEncontrados {len(client_locations)} pares cliente-localização para processar")
        
        all_results = []
        successful_count = 0
        
        # Processa cada cliente-localização sequencialmente
        for i, (client, location) in enumerate(client_locations, 1):
            print(f"\n{'='*60}")
            print(f"Processando {i}/{len(client_locations)}: {client} - {location}")
            print(f"{'='*60}")
            
            try:
                # Processa este cliente-localização
                result = self.process_client_location(client, location, days_back)
                all_results.append(result)
                
                if result['success']:
                    successful_count += 1
                    print(f"\n✓ Processado com sucesso {client} - {location}")
                else:
                    print(f"\n✗ Falha ao processar {client} - {location}")
                
                # Imprime resumo para este cliente-localização
                print(f"\nResumo para {client} - {location}:")
                print(f"  Câmeras carregadas: {result['cameras_loaded']}")
                print(f"  Câmeras com falha: {result['failing_cameras']}")
                print(f"  Horas estimadas: {result['hours_estimated']}")
                print(f"  Registros inseridos: {result['records_inserted']}")
                print(f"  Registros atualizados: {result['records_updated']}")
                
            except Exception as e:
                print(f"\n✗ Erro processando {client} - {location}: {e}")
                import traceback
                traceback.print_exc()
                # Adiciona resultado de erro
                all_results.append({
                    'client': client,
                    'location': location,
                    'success': False,
                    'error': str(e)
                })
        
        # Imprime resumo final
        print("\n" + "=" * 60)
        print("PROCESSAMENTO CONCLUÍDO")
        print("=" * 60)
        
        total_cameras = sum(r.get('cameras_loaded', 0) for r in all_results)
        total_failing = sum(r.get('failing_cameras', 0) for r in all_results)
        total_hours = sum(r.get('hours_estimated', 0) for r in all_results)
        total_inserted = sum(r.get('records_inserted', 0) for r in all_results)
        total_updated = sum(r.get('records_updated', 0) for r in all_results)
        
        print(f"\nResumo Geral:")
        print(f"  Clientes-localizações processados: {len(client_locations)}")
        print(f"  Processados com sucesso: {successful_count}")
        print(f"  Total de câmeras carregadas: {total_cameras}")
        print(f"  Total de câmeras com falha: {total_failing}")
        print(f"  Total de horas estimadas: {total_hours}")
        print(f"  Total de registros inseridos: {total_inserted}")
        print(f"  Total de registros atualizados: {total_updated}")

    def debug_three_cameras_failing(self):
        """Teste específico para o cenário de 3 câmeras falhando."""
        
        # Simular o cenário que você descreveu
        test_cameras = [148782, 155266, 155325]
        test_hour = 15  # 15:00
        
        print("\n" + "="*60)
        print("DEBUG: 3 CÂMERAS FALHANDO NO MESMO HORÁRIO")
        print("="*60)
        
        # Carregar dados para análise
        self.connect()
        
        for camera_id in test_cameras:
            print(f"\n{'='*40}")
            print(f"ANÁLISE CÂMERA {camera_id}:")
            print(f"{'='*40}")
            
            # Obter dados históricos
            hist_data = self.flow_df[
                (self.flow_df['camera_id'] == camera_id) &
                (self.flow_df['hour'] == test_hour)
            ]
            
            if len(hist_data) > 0:
                print(f"Total registros históricos: {len(hist_data)}")
                print(f"Média entrada: {hist_data['total_inside'].mean():.0f}")
                print(f"Média saída: {hist_data['total_outside'].mean():.0f}")
                print(f"Média total: {(hist_data['total_inside'] + hist_data['total_outside']).mean():.0f}")
                
                # Mostrar por dia da semana
                for weekday in range(7):
                    weekday_data = hist_data[hist_data['weekday'] == weekday]
                    if len(weekday_data) > 0:
                        avg = (weekday_data['total_inside'] + weekday_data['total_outside']).mean()
                        print(f"  Dia {weekday}: {avg:.0f}")
            else:
                print("Sem dados históricos!")
        
        # Verificar relações entre as câmeras
        print(f"\n{'='*40}")
        print("RELAÇÕES ENTRE CÂMERAS:")
        print(f"{'='*40}")
        
        for i in range(len(test_cameras)):
            for j in range(i+1, len(test_cameras)):
                cam_a = test_cameras[i]
                cam_b = test_cameras[j]
                
                ratio = self._get_hourly_ratio(cam_a, cam_b, test_hour, 0)  # Assumindo segunda-feira
                print(f"{cam_a} -> {cam_b}: ratio = {ratio:.3f}")
                print(f"  (Isso significa que {cam_b} tem {ratio:.1f}x o movimento de {cam_a})")
        
        self.disconnect()


def main():
    """Função principal de execução."""
    # Configuração
    DB_PATH = "nodehub.db"  # Atualize com o caminho do seu banco de dados
    
    # Define quais pares cliente-localização processar
    TARGET_CLIENT_LOCATIONS = [
        ('net3rcorp', 'teste'),
        # Adicione mais conforme necessário
    ]
    
    # Cria imputador
    imputer = CameraDataImputer(DB_PATH, TARGET_CLIENT_LOCATIONS)
    
    try:
        # Executa imputação para todos clientes-localizações
        imputer.run_imputation(
            days_back=45  # Usa 45 dias de dados históricos
        )
    except Exception as e:
        print(f"\n✗ Erro fatal durante imputação: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Garante que conexão seja fechada
        try:
            imputer.disconnect()
        except:
            pass

def debug_main():

    """Função principal de execução."""
    # Configuração
    DB_PATH = "nodehub.db"  # Atualize com o caminho do seu banco de dados
    
    # Define quais pares cliente-localização processar
    TARGET_CLIENT_LOCATIONS = [
        ('net3rcorp', 'teste'),
        # Adicione mais conforme necessário
    ]

    # No seu main(), adicione:
    imputer = CameraDataImputer(DB_PATH, TARGET_CLIENT_LOCATIONS)
    imputer.connect()

    # Carregar dados para um cliente-localização específico
    imputer.load_data_for_client_location('net3rcorp', 'teste', days_back=45)

    # Executar debug
    imputer.debug_three_cameras_failing()

    imputer.disconnect()
    

if __name__ == "__main__":
    # Instale pacotes necessários se não estiverem instalados
    # pip install pandas numpy
    #debug_main()

    main()