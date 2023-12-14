import javax.persistence.criteria.*;

@repository
public class GrpAgencyAorViewRepositoryCustomImpl implements GrpAgencyAorViewRepositoryCustom {

@PersistenceContext
private EntityManager entityManager;

@Override
public List<Object[]> getResults(String ageyNb, String mktSegDe) {
    CriteriaBuilder cb = entityManager.getCriteriaBuilder();
    CriteriaQuery<Object[]> query = cb.createQuery(Object[].class);
    Root<GrpAgencyAorView> main = query.from(GrpAgencyAorView.class);

    Join<GrpAgencyAorView, GrpAgencyAorView> temp = main.join("clerkCd", JoinType.LEFT);

    List<Selection<?>> selectionList = new ArrayList<>();
    selectionList.add(main.get("grpNb"));
    selectionList.add(main.get("mktSegDe"));
    selectionList.add(main.get("grpAscnCd"));
    selectionList.add(main.get("clerkCd"));
    selectionList.add(main.get("ageyNb"));
    selectionList.add(cb.count(temp.get("clerkCd")).alias("clerkCdCnt"));
    selectionList.add(cb.rowNumber().over().partitionBy(main.get("clerkCd")).orderBy(main.get("clerkCd"), cb.desc(cb.count(temp.get("clerkCd")))).alias("row1"));

    query.multiselect(selectionList.toArray(new Selection<?>[0]));

    Predicate predicate = cb.conjunction();

    if (StringUtils.isNotBlank(ageyNb)) {
        predicate = cb.and(predicate, cb.equal(main.get("ageyNb"), ageyNb));
    }

    if (StringUtils.isNotBlank(mktSegDe)) {
        predicate = cb.and(predicate, cb.equal(main.get("mktSegDe"), mktSegDe));
    }

    query.where(predicate)
            .groupBy(main.get("clerkCd"))
            .orderBy(cb.desc(cb.count(temp.get("clerkCd"))));

    TypedQuery<Object[]> typedQuery = entityManager.createQuery(query);
    return typedQuery.getResultList();
}
}


@Entity
@Table(name = "grp_agency_aor_view")
public class GrpAgencyAorView {

    @Id
    @Column(name = "grp_nb")
    private Long grpNb;

    @Column(name = "mkt_seg_de")
    private String mktSegDe;

    @Column(name = "grp_ascn_cd")
    private String grpAscnCd;

    @Column(name = "clerk_cd")
    private String clerkCd;

    @Column(name = "agey_nb")
    private String ageyNb;

    // getters and setters
}

@Repository
public interface GrpAgencyAorViewRepository extends JpaRepository<GrpAgencyAorView, Long> {

    @Query("SELECT main.grpNb, main.mktSegDe, main.grpAscnCd, main.clerkCd, main.ageyNb, temp.clerkCdCnt, " +
            "ROW_NUMBER() OVER (PARTITION BY main.clerkCd ORDER BY main.clerkCd, temp.clerkCdCnt) as row1 " +
            "FROM GrpAgencyAorView main " +
            "JOIN (SELECT clerkCd, count(*) as clerkCdCnt " +
            "FROM GrpAgencyAorView " +
            "WHERE main.ageyNb = '0052' AND main.mktSegDe = 'Small Group' " +
            "GROUP BY clerkCd) temp " +
            "ON main.clerkCd = temp.clerkCd " +
            "WHERE main.ageyNb = '0052' AND main.mktSegDe = 'Small Group' " +
            "ORDER BY temp.clerkCdCnt DESC")
    List<Object[]> getResults();
}
