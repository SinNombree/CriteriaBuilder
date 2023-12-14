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
