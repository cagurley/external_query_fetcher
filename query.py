"""
Query module
"""

QUERIES = {
    "test_scores_to_ps": """
        declare @end datetime = convert(datetime, convert(date, getdate()));
        declare @start datetime = dateadd(d, -1, @end);
        with cte as (
          select
            t.*,
            (select top(1) [value] from dbo.getFieldExportTable(t.[id], 'ug_ls_data_source')) as [ls_data_source],
            f.[value] as [emplid]
          from [test] as t
          inner join [person] as p on t.[record] = p.[id]
          inner join [field] as f on p.[id] = f.[record] and f.[field] = 'emplid'
          where t.[type] in ('ACCU', 'ACT', 'AP', 'duolingo160', 'IB', 'IELTS', 'KYOTE', 'SATII', 'SATR', 'SEMP', 'TOEFL')
          and t.[confirmed] = 1
          and t.[date] is not null
          and p.[id] not in (
            select [record]
            from [tag]
            where [tag] = 'test'
          )
          and (
            (
              (
                t.[updated] >= @start
                and t.[updated] < @end
              ) or (
                f.[timestamp] >= @start
                and f.[timestamp] < @end
              )
            ) or exists (
              select *
              from [audit]
              where p.[id] = [record]
              and (
                [type] = 'merge'
                or (
                  t.[id] = [entity]
                  and [message] like 'Test Score Updated%'
                )
              )
              and [timestamp] >= @start
              and [timestamp] < @end
            )
          )
          and (
            t.[source] not in (
              select [id]
              from [source]
              where [format] in (
                dbo.toGuidString('1EF6DBCE-CF4D-4DE4-932A-423891FBA9FD'),
                dbo.toGuidString('BE8DD60A-0271-46C6-861F-815C26D360A2'),
                dbo.toGuidString('824EC353-0321-4B3C-9BE7-CAA4EF0AFC82'),
                dbo.toGuidString('0D8658FC-CE3C-4B85-8033-652AD495C58F')
              )
            )
            or t.[source] is null
          )
        )
        select distinct
          t.[emplid],
          dbo.toGuidString(t.[id]) as [id],
          t.[test],
          t.[component],
          t.[date],
          (
            case
            when t.[ls_data_source] is null then 'AO'
            else t.[ls_data_source]
            end
          ) as [ls_data_source],
          t.[score],
          t.[created],
          t.[updated]
        from (
          select
            t.[emplid],
            t.[record],
            t.[id],
            (
              case t.[type]
              when 'AP' then 'APS'
              when 'duolingo160' then 'DUO'
              when 'SATR' then 'SATI'
              else t.[type]
              end
            ) as [test],
            (
              case t.[type]
              when 'ACT' then 'COMP'
              when 'AP' then lt.[export]
              when 'duolingo160' then 'OVRLL'
              when 'IB' then lt.[export]
              when 'IELTS' then 'TOTAL'
              when 'SATR' then 'TOTAL'
              when 'TOEFL' then (
                case t.[subtype]
                when 'ESS' then 'TOTE'
                else 'TOTAL'
                end
              )
              else t.[subtype]
              end
            ) as [component],
            t.[date],
            (select top(1) [value] from dbo.getFieldExportTable(t.[id], 'ug_ls_data_source')) as [ls_data_source],
            t.[total] as [score],
            t.[created],
            t.[updated],
            t.[source]
          from [cte] as t
          left outer join [lookup.test] as lt on t.[type] = lt.[id] and t.[subtype] = lt.[subtype]
          where [type] in ('ACCU', 'ACT', 'AP', 'duolingo160', 'IB', 'IELTS', 'KYOTE', 'SATII', 'SATR', 'SEMP', 'TOEFL')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'duolingo160' then 'DUO'
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'ENGL'
              when [type] = 'duolingo160' then 'LITER'
              when [type] = 'IELTS' then 'LIST'
              when [type] = 'SATI' then 'VERB'
              when [type] = 'SATR' then 'ERWS'
              when [type] = 'TOEFL' then (
                case [subtype]
                when 'ESS' then 'LISTE'
                else 'LIST'
                end
              )
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score1],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'duolingo160', 'IELTS', 'SATI', 'SATR', 'TOEFL')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'duolingo160' then 'DUO'
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'MATH'
              when [type] = 'duolingo160' then 'CONV'
              when [type] = 'IELTS' then 'READ'
              when [type] = 'SATI' then 'MATH'
              when [type] = 'SATR' then 'MSS'
              when [type] = 'TOEFL' then (
                case [subtype]
                when 'ESS' then 'READE'
                else 'READ'
                end
              )
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score2],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'duolingo160', 'IELTS', 'SATI', 'SATR', 'TOEFL')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'duolingo160' then 'DUO'
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'READ'
              when [type] = 'duolingo160' then 'COMPR'
              when [type] = 'IELTS' then 'WRITE'
              when [type] = 'SATI' then 'WR'
              when [type] = 'SATR' then 'RT'
              when [type] = 'TOEFL' then (
                case [subtype]
                when 'ESS' then 'WRE'
                else 'WR'
                end
              )
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score3],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'duolingo160', 'IELTS', 'SATI', 'SATR', 'TOEFL')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'duolingo160' then 'DUO'
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'SCIRE'
              when [type] = 'duolingo160' then 'PROD'
              when [type] = 'IELTS' then 'SPEAK'
              when [type] = 'SATI' then 'ES'
              when [type] = 'SATR' then 'WLT'
              when [type] = 'TOEFL' then (
                case [subtype]
                when 'ESS' then 'SPKE'
                when 'IBT' then 'SPEAK'
                when 'PBT' then 'TWE'
                end
              )
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score4],
            [created],
            [updated],
            [source]
          from [cte] as t
          where (
            [type] in ('ACT', 'duolingo160', 'IELTS', 'SATI', 'SATR')
            or (
              [type] = 'TOEFL'
              and [subtype] in ('ESS', 'IBT', 'PBT')
            )
          )
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'WR15'
              when [type] = 'SATI' then 'MC'
              when [type] = 'SATR' then 'MT'
              when [type] = 'TOEFL' then 'SCE'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score5],
            [created],
            [updated],
            [source]
          from [cte] as t
          where (
            [type] in ('ACT', 'SATI', 'SATR')
            or (
              [type] = 'TOEFL'
              and [subtype] = 'ESS'
            )
          )
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'EW'
              when [type] = 'SATR' then 'ASC'
              when [type] = 'TOEFL' then 'VKE'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score6],
            [created],
            [updated],
            [source]
          from [cte] as t
          where (
            [type] in ('ACT', 'SATR')
            or (
              [type] = 'TOEFL'
              and [subtype] = 'ESS'
            )
          )
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'WRIDE'
              when [type] = 'SATR' then 'AHSSC'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score7],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'WRDEV'
              when [type] = 'SATR' then 'RWC'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score8],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'WRORG'
              when [type] = 'SATR' then 'CE'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score9],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'WRLAN'
              when [type] = 'SATR' then 'EI'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score10],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'ELA'
              when [type] = 'SATR' then 'SEC'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score11],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'STEM'
              when [type] = 'SATR' then 'HA'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score12],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            (
              case
              when [type] = 'SATR' then 'SATI'
              else [type]
              end
            ),
            (
              case
              when [type] = 'ACT' then 'WR'
              when [type] = 'SATR' then 'PAM'
              else ''
              end
            ),
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score13],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] in ('ACT', 'SATR')
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            'SATI',
            'PSDA',
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score14],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] = 'SATR'
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            'SATI',
            'ESR',
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score15],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] = 'SATR'
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            'SATI',
            'ESA',
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score16],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] = 'SATR'
          and [confirmed] = 1
          union
          select
            [emplid],
            [record],
            [id],
            'SATI',
            'ESW',
            [date],
            (select top(1) [value] from dbo.getFieldExportTable([id], 'ug_ls_data_source')),
            [score17],
            [created],
            [updated],
            [source]
          from [cte] as t
          where [type] = 'SATR'
          and [confirmed] = 1
        ) as t
        where t.[score] is not null
        order by t.[test], t.[updated] desc, t.[created] desc;
    """
}
